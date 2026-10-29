package client

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sadrax4/kvorum/api/kvorumpb"
)

// unaryCall performs one attempt of a request against a connection.
type unaryCall func(ctx context.Context, cc *grpc.ClientConn) error

// invoke runs call against the preferred endpoint and retries according to
// the retry policy:
//
//   - a LeaderHint detail with an address switches to that address and retries
//     at once;
//   - an Unavailable, Aborted or per-attempt DeadlineExceeded error rotates to
//     the next endpoint after an exponential backoff with jitter;
//   - any other error is returned to the caller unchanged (wrapped).
//
// Hint follows and backoff retries both count against MaxRetries, which bounds
// the total number of attempts to MaxRetries+1.
func (c *Client) invoke(ctx context.Context, op string, call unaryCall) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("client: %s: %w", op, err)
	}
	addr := c.endpoints.current()
	var lastErr error
	for attempt := 0; ; attempt++ {
		cc, err := c.conn(addr)
		if err != nil {
			return err
		}
		attemptCtx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
		err = call(attemptCtx, cc)
		cancel()
		if err == nil {
			c.endpoints.prefer(addr)
			return nil
		}
		lastErr = err
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("client: %s: %w (last error: %w)", op, ctxErr, err)
		}
		leader, hinted := leaderHint(err)
		if !hinted && !isRetryable(err) {
			return fmt.Errorf("client: %s: %w", op, err)
		}
		if attempt >= c.cfg.MaxRetries {
			break
		}
		if hinted && leader != addr {
			c.log.Debug("following leader hint", "op", op, "from", addr, "to", leader)
			c.endpoints.prefer(leader)
			addr = leader
			continue
		}
		next := c.endpoints.next(addr)
		delay := c.backoff(attempt)
		c.log.Debug("retrying request",
			"op", op, "attempt", attempt+1, "endpoint", addr, "next", next, "delay", delay, "error", err)
		addr = next
		if err := sleep(ctx, delay); err != nil {
			return fmt.Errorf("client: %s: %w (last error: %w)", op, err, lastErr)
		}
	}
	return fmt.Errorf("client: %s: giving up after %d attempts: %w", op, c.cfg.MaxRetries+1, lastErr)
}

// backoff returns the jittered delay before retry number attempt+1. The delay
// is drawn uniformly from [d/2, d], where d doubles with every attempt from
// RetryBaseBackoff up to RetryMaxBackoff.
func (c *Client) backoff(attempt int) time.Duration {
	d := c.cfg.RetryBaseBackoff
	for i := 0; i < attempt && d < c.cfg.RetryMaxBackoff; i++ {
		d *= 2
	}
	if d > c.cfg.RetryMaxBackoff {
		d = c.cfg.RetryMaxBackoff
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(half+1) //nolint:gosec // backoff jitter needs no cryptographic randomness
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// leaderHint extracts the leader address carried by an Unavailable or
// FailedPrecondition error. It returns false when the error carries no hint
// or the hint names no address.
func leaderHint(err error) (string, bool) {
	st, ok := status.FromError(err)
	if !ok {
		return "", false
	}
	if st.Code() != codes.Unavailable && st.Code() != codes.FailedPrecondition {
		return "", false
	}
	for _, d := range st.Details() {
		if h, ok := d.(*kvorumpb.LeaderHint); ok && h.GetLeaderAddress() != "" {
			return h.GetLeaderAddress(), true
		}
	}
	return "", false
}

// isRetryable reports whether an error that carries no leader hint should be
// retried against another endpoint. Connection failures surface as
// Unavailable; a per-attempt timeout surfaces as DeadlineExceeded (the
// caller's context is checked separately); Aborted covers writes that were
// interrupted by a leadership change and are safe to resend thanks to the
// Session.
func isRetryable(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		return true
	default:
		return false
	}
}
