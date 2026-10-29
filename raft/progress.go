package raft

import "fmt"

// Progress is the leader's view of one follower's log. The leader uses it to
// decide what to send next and how much to send before waiting for an
// acknowledgement.
//
// Followers move between three states:
//
//   - probe: the leader does not know where the follower's log diverges. It
//     sends one MsgApp at a time and waits for the response before sending
//     more, moving Next backwards on rejections.
//   - replicate: the follower's log matches up to Match. The leader streams
//     entries optimistically, keeping up to maxInflight messages in flight.
//   - snapshot: the follower is too far behind for the leader's log and a
//     snapshot is being installed. Nothing else is sent until the transport
//     reports the result.
type Progress struct {
	// Match is the highest index known to be replicated on the follower.
	Match Index
	// Next is the index of the next entry to send.
	Next Index

	State ProgressStateType

	// PendingSnapshot is the index of the snapshot being installed while in
	// the snapshot state.
	PendingSnapshot Index

	// RecentActive is true if the follower has responded since the last
	// CheckQuorum tick. It is reset by the leader at every election timeout.
	RecentActive bool

	// MsgAppFlowPaused is true in the probe state when a message has been
	// sent and the leader is waiting for its response.
	MsgAppFlowPaused bool

	// Inflights tracks optimistic sends in the replicate state.
	Inflights *inflights

	IsLearner bool
}

func newProgress(next Index, maxInflight int, isLearner bool) *Progress {
	return &Progress{
		Next:      next,
		Inflights: newInflights(maxInflight),
		IsLearner: isLearner,
		// A freshly added member is considered active so that CheckQuorum
		// does not step the leader down before the member had a chance to
		// respond.
		RecentActive: true,
	}
}

func (pr *Progress) resetState(state ProgressStateType) {
	pr.MsgAppFlowPaused = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.Inflights.reset()
}

// becomeProbe transitions into the probe state. From the snapshot state it
// resumes just after the snapshot; otherwise it resumes after Match.
func (pr *Progress) becomeProbe() {
	if pr.State == ProgressSnapshot {
		pending := pr.PendingSnapshot
		pr.resetState(ProgressProbe)
		pr.Next = max(pr.Match+1, pending+1)
		return
	}
	pr.resetState(ProgressProbe)
	pr.Next = pr.Match + 1
}

func (pr *Progress) becomeReplicate() {
	pr.resetState(ProgressReplicate)
	pr.Next = pr.Match + 1
}

func (pr *Progress) becomeSnapshot(snapshotIndex Index) {
	pr.resetState(ProgressSnapshot)
	pr.PendingSnapshot = snapshotIndex
}

// maybeUpdate records that the follower acknowledged entries up to n. It
// returns true if Match advanced.
func (pr *Progress) maybeUpdate(n Index) bool {
	updated := false
	if pr.Match < n {
		pr.Match = n
		updated = true
		pr.MsgAppFlowPaused = false
	}
	pr.Next = max(pr.Next, n+1)
	return updated
}

// optimisticUpdate assumes entries up to n will be accepted and moves Next
// past them (replicate state).
func (pr *Progress) optimisticUpdate(n Index) {
	pr.Next = n + 1
}

// maybeDecrTo handles a rejected MsgApp whose PrevLogIndex was rejected and
// moves Next back to nextHint. It returns false if the rejection is stale and
// should be ignored.
func (pr *Progress) maybeDecrTo(rejected, nextHint Index) bool {
	if pr.State == ProgressReplicate {
		// The rejection must be stale if the progress has matched and
		// "rejected" is smaller than Match.
		if rejected <= pr.Match {
			return false
		}
		// Directly decrease next to match + 1.
		pr.Next = pr.Match + 1
		return true
	}
	// The rejection must be stale if "rejected" does not match Next-1: the
	// leader has already moved on.
	if pr.Next-1 != rejected {
		return false
	}
	pr.Next = max(min(rejected, nextHint), 1)
	pr.MsgAppFlowPaused = false
	return true
}

// isPaused reports whether the leader should hold off sending to this
// follower.
func (pr *Progress) isPaused() bool {
	switch pr.State {
	case ProgressProbe:
		return pr.MsgAppFlowPaused
	case ProgressReplicate:
		return pr.Inflights.full()
	case ProgressSnapshot:
		return true
	default:
		panic(fmt.Sprintf("raft: unexpected progress state %d", pr.State))
	}
}

// sentEntries updates the progress after a MsgApp carrying entries up to
// lastIndex was sent.
func (pr *Progress) sentEntries(lastIndex Index) {
	switch pr.State {
	case ProgressReplicate:
		pr.optimisticUpdate(lastIndex)
		pr.Inflights.add(lastIndex)
	case ProgressProbe:
		pr.MsgAppFlowPaused = true
	}
}

func (pr *Progress) status() ProgressStatus {
	return ProgressStatus{
		Match:        pr.Match,
		Next:         pr.Next,
		State:        pr.State,
		RecentActive: pr.RecentActive,
		IsLearner:    pr.IsLearner,
	}
}

func (pr *Progress) String() string {
	return fmt.Sprintf("%s match=%d next=%d active=%t paused=%t inflight=%d learner=%t",
		pr.State, pr.Match, pr.Next, pr.RecentActive, pr.isPaused(), pr.Inflights.count(), pr.IsLearner)
}

// inflights is a sliding window of the last indexes of MsgApp messages sent
// optimistically to a follower. It is a fixed-capacity ring buffer.
type inflights struct {
	start  int
	size   int
	buffer []Index
}

func newInflights(capacity int) *inflights {
	return &inflights{buffer: make([]Index, capacity)}
}

func (in *inflights) add(last Index) {
	if in.full() {
		panic("raft: cannot add into a full inflights window")
	}
	next := in.start + in.size
	if next >= len(in.buffer) {
		next -= len(in.buffer)
	}
	in.buffer[next] = last
	in.size++
}

// freeLE releases every in-flight message whose last index is at most to.
func (in *inflights) freeLE(to Index) {
	if in.size == 0 || to < in.buffer[in.start] {
		return
	}
	idx := in.start
	freed := 0
	for range in.size {
		if to < in.buffer[idx] {
			break
		}
		freed++
		idx++
		if idx >= len(in.buffer) {
			idx -= len(in.buffer)
		}
	}
	in.size -= freed
	in.start = idx
	if in.size == 0 {
		in.start = 0
	}
}

// freeFirstOne releases the oldest in-flight message. It is used when a
// heartbeat response proves the follower is alive while the window is full,
// so replication can resume if a MsgAppResp was lost.
func (in *inflights) freeFirstOne() {
	if in.size == 0 {
		return
	}
	in.freeLE(in.buffer[in.start])
}

func (in *inflights) full() bool { return in.size == len(in.buffer) }
func (in *inflights) count() int { return in.size }
func (in *inflights) reset()     { in.start, in.size = 0, 0 }
