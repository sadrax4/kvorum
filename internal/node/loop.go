package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sadrax4/kvorum/internal/kv"
	"github.com/sadrax4/kvorum/raft"
)

// maxProposalBatch bounds how many queued proposals are appended before the
// loop produces a Ready, so that latency stays bounded under load.
const maxProposalBatch = 256

// run is the node's single raft-driving goroutine.
func (n *Node) run() {
	defer close(n.doneC)
	defer n.shutdown()

	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.rn.Tick()
			n.ticks++
			n.onTick()
		case m := <-n.msgC:
			n.stepMessage(m)
		case p := <-n.proposeC:
			n.submitProposal(p)
			for range maxProposalBatch {
				select {
				case p := <-n.proposeC:
					n.submitProposal(p)
				default:
				}
			}
		case r := <-n.readC:
			n.submitRead(r)
		case rep := <-n.reportC:
			if rep.snapshot {
				n.rn.ReportSnapshot(rep.id, rep.ok)
			} else {
				n.rn.ReportUnreachable(rep.id)
			}
		case tr := <-n.transferC:
			tr.resC <- n.rn.TransferLeader(tr.to)
		case req := <-n.snapshotC:
			idx, err := n.takeSnapshot()
			req.resC <- snapshotResult{index: idx, err: err}
		case r := <-n.publishC:
			n.publishInFlight = false
			if r.ok {
				n.publishedAt = r.term
			}
		case <-n.stopC:
			return
		}
		if err := n.processReady(); err != nil {
			// Storage failures are not recoverable: the node must not keep
			// acknowledging writes it cannot persist. Stop serving entirely
			// rather than linger as a member that looks alive.
			n.logger.Error("fatal error while processing raft output; stopping node", "error", err)
			n.fatalErr.Store(&err)
			n.stopOnce.Do(func() { close(n.stopC) })
			n.mu.Lock()
			n.leader, n.state, n.progress = raft.None, raft.StateFollower, nil
			n.mu.Unlock()
			n.failWaiters(err)
			n.failReads(err)
			return
		}
		n.maybeSnapshot()
	}
}

func (n *Node) shutdown() {
	n.transport.Stop()
	if err := n.wal.Close(); err != nil {
		n.logger.Error("closing wal", "error", err)
	}
	n.failWaiters(ErrStopped)
	n.failReads(ErrStopped)
	n.logger.Info("node stopped")
}

func (n *Node) stepMessage(m raft.Message) {
	if err := n.rn.Step(m); err != nil {
		if errors.Is(err, raft.ErrStepPeerNotFound) {
			n.logger.Debug("dropping message from unknown peer", "from", m.From, "type", m.Type)
			return
		}
		n.logger.Warn("raft rejected message", "type", m.Type, "from", m.From, "error", err)
	}
}

func (n *Node) submitProposal(p *proposal) {
	if p.ctx.Err() != nil {
		p.resolve(kv.Result{}, p.ctx.Err())
		return
	}
	var (
		idx raft.Index
		err error
	)
	switch p.kind {
	case proposalConfChange:
		idx, err = n.rn.ProposeConfChange(p.cc)
	default:
		idx, err = n.rn.Propose(p.data)
	}
	if err != nil {
		n.metrics.RaftProposals.WithLabelValues("not_leader").Inc()
		p.resolve(kv.Result{}, translateProposeError(err))
		return
	}
	p.index = idx
	p.term = n.rn.BasicStatus().Term
	if prev, ok := n.waiters[idx]; ok {
		// Cannot happen while the node is leader: indexes are unique.
		prev.resolve(kv.Result{}, ErrProposalLost)
	}
	n.waiters[idx] = p
}

func translateProposeError(err error) error {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		return ErrNotLeader
	case errors.Is(err, raft.ErrProposalDropped):
		return ErrLeadershipTransferring
	default:
		return err
	}
}

func (n *Node) submitRead(r *readWaiter) {
	if r.ctx.Err() != nil {
		r.resolve(r.ctx.Err())
		return
	}
	bs := n.rn.BasicStatus()
	if bs.Leader == raft.None {
		r.resolve(ErrNoLeader)
		return
	}
	n.readSeq++
	r.id = n.readSeq
	r.leader = bs.Leader
	n.readWaiters[r.id] = r
	n.rn.ReadIndex(encodeReadContext(r.id))
}

func encodeReadContext(id uint64) []byte {
	var b [8]byte
	for i := range 8 {
		b[7-i] = byte(id >> (8 * i))
	}
	return b[:]
}

func decodeReadContext(b []byte) (uint64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	var id uint64
	for _, c := range b {
		id = id<<8 | uint64(c)
	}
	return id, true
}

// onTick runs housekeeping that only needs to happen a few times a second.
func (n *Node) onTick() {
	if n.ticks%uint64(n.cfg.ElectionTick) == 0 {
		n.expireWaiters()
	}
	n.maybePublishClientAddress()
}

// expireWaiters drops requests whose callers have given up, so that the
// waiter maps cannot grow without bound while the node is not making
// progress.
func (n *Node) expireWaiters() {
	for idx, p := range n.waiters {
		if p.ctx.Err() != nil {
			delete(n.waiters, idx)
			n.metrics.RaftProposals.WithLabelValues("timeout").Inc()
		}
	}
	for id, r := range n.readWaiters {
		if r.ctx.Err() != nil {
			delete(n.readWaiters, id)
		}
	}
	kept := n.pendingReads[:0]
	for _, r := range n.pendingReads {
		if r.ctx.Err() == nil {
			kept = append(kept, r)
		}
	}
	n.pendingReads = kept
}

// maybePublishClientAddress records this node's client address in the
// replicated membership table once per term, so that every node can hand out
// leader hints. A leader proposes the entry itself; a follower asks the
// leader to do it through the transport's Publish RPC.
func (n *Node) maybePublishClientAddress() {
	if n.cfg.AdvertiseClient == "" || n.publishInFlight {
		return
	}
	bs := n.rn.BasicStatus()
	if n.publishedAt == bs.Term || bs.Leader == raft.None {
		return
	}
	attrs := memberAttrs{ID: n.cfg.ID, ClientAddress: n.cfg.AdvertiseClient}
	if bs.State == raft.StateLeader {
		// Publish once per term: the local membership table always knows the
		// node's own address, so it cannot tell whether followers do.
		n.publishedAt = bs.Term
		if _, err := n.rn.Propose(encodeEntry(entryKindAttrs, encodeMemberAttrs(attrs))); err != nil {
			n.logger.Debug("could not publish client address", "error", err)
			n.publishedAt = 0
		}
		return
	}
	if bs.State != raft.StateFollower {
		return
	}
	n.publishInFlight = true
	leader, term := bs.Leader, bs.Term
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := n.transport.Publish(ctx, leader, n.cfg.AdvertiseClient)
		if err != nil {
			n.logger.Debug("could not publish client address through the leader", "leader", leader, "error", err)
		}
		select {
		case n.publishC <- publishResult{term: term, ok: err == nil}:
		case <-n.stopC:
		}
	}()
}

// processReady drains raft's pending work, honouring the Ready contract.
func (n *Node) processReady() error {
	for n.rn.HasReady() {
		start := time.Now()
		rd := n.rn.Ready()

		if err := n.persistReady(rd); err != nil {
			return err
		}
		if rd.SoftState != nil {
			n.onSoftStateChange(*rd.SoftState)
		}
		n.transport.Send(rd.Messages)

		if len(rd.CommittedEntries) > 0 {
			applyStart := time.Now()
			if err := n.applyEntries(rd.CommittedEntries); err != nil {
				return err
			}
			n.metrics.RaftApplyDuration.Observe(time.Since(applyStart).Seconds())
		}
		n.onReadStates(rd.ReadStates)
		n.rn.Advance(rd)

		bs := n.rn.BasicStatus()
		n.mu.Lock()
		n.refreshStatusLocked(bs)
		if bs.State == raft.StateLeader {
			n.progress = n.rn.Status().Progress
		} else {
			n.progress = nil
		}
		n.mu.Unlock()
		n.releaseReads()
		n.metrics.RaftReadyDuration.Observe(time.Since(start).Seconds())
	}
	return nil
}

func (n *Node) onSoftStateChange(ss raft.SoftState) {
	n.mu.Lock()
	prevLeader, prevState := n.leader, n.state
	n.leader, n.state = ss.Leader, ss.State
	n.mu.Unlock()

	if ss.Leader != prevLeader {
		n.metrics.RaftLeaderChanges.Inc()
		n.logger.Info("leader changed", "leader", ss.Leader, "state", ss.State)
		// Reads forwarded to the previous leader will never be answered by
		// the new one; fail them quickly so that clients retry instead of
		// waiting for their deadline. Reads that already hold a read index
		// stay valid and are released once the index is applied.
		for id, r := range n.readWaiters {
			if r.leader != ss.Leader {
				delete(n.readWaiters, id)
				r.resolve(ErrNoLeader)
			}
		}
	}
	if prevState == raft.StateLeader && ss.State != raft.StateLeader {
		// Proposals accepted by this node may still commit through the new
		// leader, but their outcome can no longer be observed here. Clients
		// retry with the same session, which makes the retry exactly-once.
		n.failWaiters(ErrProposalLost)
		n.failReads(ErrNotLeader)
		n.publishedAt = 0
	}
}

// applyEntries applies committed entries to the state machine and resolves
// the proposals waiting for them.
func (n *Node) applyEntries(ents []raft.Entry) error {
	for _, e := range ents {
		var res kv.Result
		switch {
		case e.Type == raft.EntryConfChange:
			if err := n.applyConfChange(e); err != nil {
				return err
			}
		case len(e.Data) == 0:
			// Leader no-op.
		default:
			res = n.applyNormal(e)
		}
		n.appliedIndex.Store(uint64(e.Index))
		if p, ok := n.waiters[e.Index]; ok {
			delete(n.waiters, e.Index)
			if p.term == e.Term {
				n.metrics.RaftProposals.WithLabelValues("committed").Inc()
				p.resolve(res, res.Err)
			} else {
				n.metrics.RaftProposals.WithLabelValues("lost").Inc()
				p.resolve(kv.Result{}, ErrProposalLost)
			}
		}
	}
	return nil
}

// applyNormal applies a command or attribute entry. Malformed entries are
// skipped rather than fatal: every node sees the same bytes, so they all
// skip identically and stay consistent.
func (n *Node) applyNormal(e raft.Entry) kv.Result {
	switch e.Data[0] {
	case entryKindCommand:
		cmd, err := kv.UnmarshalCommand(e.Data[1:])
		if err != nil {
			n.logger.Error("skipping malformed command in log", "index", e.Index, "error", err)
			return kv.Result{Err: err}
		}
		res := n.store.Apply(uint64(e.Index), cmd)
		if res.Err != nil && !errors.Is(res.Err, kv.ErrStaleSequence) {
			n.logger.Error("state machine rejected command", "index", e.Index, "error", res.Err)
		}
		n.metrics.KVKeys.Set(float64(n.store.Len()))
		return res
	case entryKindAttrs:
		attrs, err := decodeMemberAttrs(e.Data[1:])
		if err != nil {
			n.logger.Error("skipping malformed member attributes in log", "index", e.Index, "error", err)
			return kv.Result{}
		}
		n.mu.Lock()
		// Attributes for a node that is not a member at this point of the
		// log are ignored identically on every node, so phantom ids never
		// enter the table.
		if n.confState.Contains(attrs.ID) {
			m := n.members[attrs.ID]
			m.ClientAddress = attrs.ClientAddress
			n.members[attrs.ID] = m
		}
		n.mu.Unlock()
		return kv.Result{}
	default:
		n.logger.Error("skipping entry of unknown kind", "index", e.Index, "kind", e.Data[0])
		return kv.Result{}
	}
}

func (n *Node) applyConfChange(e raft.Entry) error {
	cc, err := raft.UnmarshalConfChange(e.Data)
	if err != nil {
		return fmt.Errorf("node: malformed configuration change at index %d: %w", e.Index, err)
	}
	cs := n.rn.ApplyConfChange(cc)
	mc, err := decodeMemberContext(cc.Context)
	if err != nil {
		n.logger.Error("ignoring member addresses in configuration change", "index", e.Index, "error", err)
	}

	n.mu.Lock()
	n.confState = cs.Clone()
	switch cc.Type {
	case raft.ConfChangeAddVoter, raft.ConfChangeAddLearner:
		existing := n.members[cc.NodeID]
		if mc.RaftAddress != "" {
			existing.RaftAddress = mc.RaftAddress
		}
		if mc.ClientAddress != "" {
			existing.ClientAddress = mc.ClientAddress
		}
		n.members[cc.NodeID] = existing
	case raft.ConfChangeRemoveNode:
		if cc.NodeID != n.cfg.ID {
			delete(n.members, cc.NodeID)
		}
	}
	n.mu.Unlock()
	n.metrics.RaftConfChanges.Inc()
	n.logger.Info("applied configuration change", "change", cc.String(), "index", e.Index, "conf", cs.String())

	n.syncPeers()
	if cc.Type == raft.ConfChangeRemoveNode && cc.NodeID == n.cfg.ID {
		n.logger.Warn("this node has been removed from the cluster")
		n.removedOnce.Do(func() { close(n.removedC) })
	}
	return nil
}

// syncPeers reconciles the transport's peer table with the membership.
func (n *Node) syncPeers() {
	n.mu.RLock()
	want := make(map[raft.NodeID]string, len(n.members))
	for _, id := range append(append([]raft.NodeID(nil), n.confState.Voters...), n.confState.Learners...) {
		if id == n.cfg.ID {
			continue
		}
		if mc, ok := n.members[id]; ok && mc.RaftAddress != "" {
			want[id] = mc.RaftAddress
		}
	}
	n.mu.RUnlock()

	for id, addr := range want {
		n.transport.AddPeer(id, addr)
	}
	for id := range n.transport.Peers() {
		if _, ok := want[id]; !ok {
			n.transport.RemovePeer(id)
		}
	}
}

// onReadStates matches confirmed read indexes with their waiters.
func (n *Node) onReadStates(states []raft.ReadState) {
	for _, rs := range states {
		id, ok := decodeReadContext(rs.Context)
		if !ok {
			continue
		}
		r, ok := n.readWaiters[id]
		if !ok {
			continue // duplicate or abandoned
		}
		delete(n.readWaiters, id)
		r.index = rs.Index
		n.pendingReads = append(n.pendingReads, r)
	}
}

// releaseReads wakes reads whose index has been applied.
func (n *Node) releaseReads() {
	if len(n.pendingReads) == 0 {
		return
	}
	applied := raft.Index(n.appliedIndex.Load())
	kept := n.pendingReads[:0]
	for _, r := range n.pendingReads {
		if r.index <= applied {
			r.resolve(nil)
		} else {
			kept = append(kept, r)
		}
	}
	n.pendingReads = kept
}

func (n *Node) failWaiters(err error) {
	for idx, p := range n.waiters {
		delete(n.waiters, idx)
		p.resolve(kv.Result{}, err)
	}
}

func (n *Node) failReads(err error) {
	for id, r := range n.readWaiters {
		delete(n.readWaiters, id)
		r.resolve(err)
	}
	for _, r := range n.pendingReads {
		r.resolve(err)
	}
	n.pendingReads = nil
}

// maybeSnapshot takes a snapshot once enough entries have been applied since
// the last one.
func (n *Node) maybeSnapshot() {
	applied := n.appliedIndex.Load()
	if applied-n.snapshotIndex.Load() < n.cfg.SnapshotThreshold || n.ticks < n.snapshotRetryTick {
		return
	}
	if _, err := n.takeSnapshot(); err != nil {
		// Back off for several election timeouts; a persistent failure
		// (full disk, unwritable directory) must not turn every loop
		// iteration into a full serialisation of the store.
		n.snapshotRetryTick = n.ticks + uint64(n.cfg.ElectionTick)*10
		n.logger.Error("snapshot failed; will retry later", "error", err, "retry_in_ticks", n.snapshotRetryTick-n.ticks)
	}
}
