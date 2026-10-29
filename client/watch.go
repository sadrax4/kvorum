package client

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sadrax4/kvorum/api/kvorumpb"
)

// watchBuffer is the capacity of the channel returned by Watch. A consumer
// that falls further behind than this applies backpressure to the stream.
const watchBuffer = 128

// Watch streams the changes to key, or to a range when WithPrefix or
// WithRange is given, starting from the moment the server registers the
// watch. Events arrive on the returned channel. The watch follows leader
// hints and reconnects with backoff when its stream fails; the stream resumes
// from the moment it is re-registered, so changes made during a reconnection
// are not replayed. The channel is closed after a final event whose Err field
// is set, either because the watch failed permanently, because ctx ended, or
// because the client was closed.
//
// Watch returns an error only when the client is closed; failures to open
// the stream are reported through the channel.
func (c *Client) Watch(ctx context.Context, key []byte, opts ...WatchOption) (<-chan WatchEvent, error) {
	o := applyWatchOptions(opts)
	rangeEnd := watchRangeEnd(key, o)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	c.watchers.Add(1)
	c.mu.Unlock()

	ch := make(chan WatchEvent, watchBuffer)
	req := &kvorumpb.WatchRequest{Key: key, RangeEnd: rangeEnd}
	go c.runWatch(ctx, req, ch)
	return ch, nil
}

// runWatch owns the watch goroutine: it opens the stream, pumps events, and
// reconnects until the watch ends for good.
func (c *Client) runWatch(ctx context.Context, req *kvorumpb.WatchRequest, ch chan<- WatchEvent) {
	defer c.watchers.Done()
	defer close(ch)

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopOnClose := context.AfterFunc(c.closeCtx, cancel)
	defer stopOnClose()

	addr := c.endpoints.current()
	failures := 0
	for {
		err := c.watchOnce(wctx, addr, req, ch, &failures)
		if wctx.Err() != nil {
			c.finishWatch(ch, c.watchEndError(ctx))
			return
		}
		leader, hinted := leaderHint(err)
		if !hinted && !isReconnectable(err) {
			c.finishWatch(ch, fmt.Errorf("client: watch: %w", err))
			return
		}
		if failures >= c.cfg.MaxRetries {
			c.finishWatch(ch, fmt.Errorf("client: watch: giving up after %d attempts: %w", failures+1, err))
			return
		}
		failures++
		if hinted && leader != addr {
			c.log.Debug("watch following leader hint", "from", addr, "to", leader)
			c.endpoints.prefer(leader)
			addr = leader
			continue
		}
		next := c.endpoints.next(addr)
		delay := c.backoff(failures - 1)
		c.log.Debug("watch reconnecting", "endpoint", addr, "next", next, "delay", delay, "error", err)
		addr = next
		if err := sleep(wctx, delay); err != nil {
			c.finishWatch(ch, c.watchEndError(ctx))
			return
		}
	}
}

// watchOnce opens one stream and delivers its events until it fails. The
// failure counter is reset after every message received, so only consecutive
// failures count towards giving up.
func (c *Client) watchOnce(ctx context.Context, addr string, req *kvorumpb.WatchRequest, ch chan<- WatchEvent, failures *int) error {
	cc, err := c.conn(addr)
	if err != nil {
		return err
	}
	stream, err := kvorumpb.NewKVServiceClient(cc).Watch(ctx, req)
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		*failures = 0
		c.endpoints.prefer(addr)
		header := headerFromProto(resp.GetHeader())
		for _, ev := range resp.GetEvents() {
			if ev == nil {
				continue
			}
			kv := kvFromProto(ev.GetKv())
			if kv == nil {
				kv = &KeyValue{}
			}
			out := WatchEvent{
				Type:   eventTypeFromProto(ev.GetType()),
				KV:     *kv,
				PrevKV: kvFromProto(ev.GetPrevKv()),
				Header: header,
			}
			select {
			case ch <- out:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// watchEndError describes why a watch ended when its context was cancelled.
func (c *Client) watchEndError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("client: watch: %w", err)
	}
	return ErrClosed
}

// finishWatch delivers the terminal event without blocking forever: when the
// consumer has stopped draining the channel the event is dropped and the
// channel is simply closed.
func (c *Client) finishWatch(ch chan<- WatchEvent, err error) {
	select {
	case ch <- WatchEvent{Err: err}:
	default:
		c.log.Debug("watch terminal event dropped: consumer not draining", "error", err)
	}
}

// isReconnectable reports whether a watch stream failure warrants a new
// stream. A clean end of stream (io.EOF) means the server released the watch,
// for example because the watcher overflowed, and the watch is re-registered.
func isReconnectable(err error) bool {
	if errors.Is(err, io.EOF) || isRetryable(err) {
		return true
	}
	// ResourceExhausted is how the server ends a watch whose events could
	// not be delivered in time (or that was reset by a snapshot restore);
	// re-registering is the documented recovery.
	return status.Code(err) == codes.ResourceExhausted
}

// unboundedRangeEnd is the wire convention for "every key from the start
// key onwards": a range end of a single zero byte, which no real key can be
// smaller than.
var unboundedRangeEnd = []byte{0}

// watchRangeEnd resolves the range end of a watch. A prefix whose PrefixEnd
// is unbounded (an empty prefix or one made only of 0xff bytes) must not
// degrade into a single-key watch, so it is sent as the unbounded marker.
func watchRangeEnd(key []byte, o watchOptions) []byte {
	if !o.prefix {
		return o.rangeEnd
	}
	if end := PrefixEnd(key); end != nil {
		return end
	}
	return unboundedRangeEnd
}
