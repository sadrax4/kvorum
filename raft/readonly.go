package raft

// readOnly tracks pending ReadIndex requests on the leader.
//
// The ReadIndex protocol (§6.4 of the Raft dissertation) works as follows: the
// leader records its current commit index as the read index for the request,
// then confirms it is still the leader by collecting heartbeat responses from
// a quorum that carry the request's context. Once confirmed, any state at or
// beyond the read index is guaranteed to reflect every write that was
// acknowledged before the request arrived.
//
// Requests are kept in arrival order. A heartbeat acknowledgement for a later
// request also proves leadership for every earlier one, so when a request is
// confirmed, all requests queued before it are released as well.
type readOnly struct {
	pending map[string]*readIndexStatus
	queue   []string
}

type readIndexStatus struct {
	req   Message
	index Index
	acks  map[NodeID]struct{}
}

func newReadOnly() *readOnly {
	return &readOnly{pending: make(map[string]*readIndexStatus)}
}

// addRequest queues a read request with the given read index. Duplicate
// contexts are ignored.
func (ro *readOnly) addRequest(index Index, m Message) {
	key := string(m.Context)
	if _, ok := ro.pending[key]; ok {
		return
	}
	ro.pending[key] = &readIndexStatus{req: m, index: index, acks: make(map[NodeID]struct{})}
	ro.queue = append(ro.queue, key)
}

// recvAck records that node id acknowledged the heartbeat carrying context and
// returns the set of acknowledging nodes, or nil if the context is unknown.
func (ro *readOnly) recvAck(id NodeID, context []byte) map[NodeID]struct{} {
	rs, ok := ro.pending[string(context)]
	if !ok {
		return nil
	}
	rs.acks[id] = struct{}{}
	return rs.acks
}

// advance releases every request queued up to and including the one with the
// given context, in order. It returns nil if the context is unknown.
func (ro *readOnly) advance(context []byte) []*readIndexStatus {
	key := string(context)
	pos := -1
	for i, k := range ro.queue {
		if k == key {
			pos = i
			break
		}
	}
	if pos < 0 {
		return nil
	}
	released := make([]*readIndexStatus, 0, pos+1)
	for _, k := range ro.queue[:pos+1] {
		released = append(released, ro.pending[k])
		delete(ro.pending, k)
	}
	ro.queue = append([]string(nil), ro.queue[pos+1:]...)
	return released
}

// lastPendingRequestCtx returns the context of the most recent pending
// request, which heartbeats carry so that acknowledgements keep flowing even
// if the original heartbeat round was lost.
func (ro *readOnly) lastPendingRequestCtx() []byte {
	if len(ro.queue) == 0 {
		return nil
	}
	return []byte(ro.queue[len(ro.queue)-1])
}
