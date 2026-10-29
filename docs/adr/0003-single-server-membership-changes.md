# ADR 0003: Single-server membership changes instead of joint consensus

**Status:** accepted

## Context

The Raft paper describes joint consensus for arbitrary membership changes.
Ongaro's dissertation (§4.1) shows that changes that add or remove a single
server at a time never allow two disjoint majorities, which makes a much
simpler protocol safe, provided two rules are respected: only one change may
be in progress, and a leader must have committed an entry of its own term
before it proposes a change (the 2015 correction to the dissertation).

## Decision

kvorum implements single-server changes with learners:

- `ConfChangeAddLearner` adds a non-voting member that receives the log;
  `ConfChangeAddVoter` adds a voter or promotes a learner;
  `ConfChangeRemoveNode` removes either.
- The leader rejects a change while the previous one is not yet applied
  (`ErrConfChangePending`) and until it has committed an entry of its term
  (`ErrLeaderTermNotCommitted`).
- The new configuration takes effect when the entry is *applied* (etcd's
  approach), not when it is appended. With the two rules above this is safe
  and keeps the leader's bookkeeping simple.
- A leader that applies its own removal steps down immediately so that the
  remaining voters can elect a replacement without waiting for a timeout.
- A follower accepts a snapshot whose membership does not include it: that is
  the normal situation for a node that joins after the leader's last
  snapshot, and the entries following the snapshot carry its own membership
  entry.

Operators are expected to add nodes as learners first and promote them once
caught up, which avoids a quorum that temporarily includes a lagging voter.

## Consequences

- Replacing several nodes requires several sequential changes.
- The membership simulator scenario exercises adds, promotions, and removals
  (including of the leader) under faults.
- Member addresses travel in the change's context and are persisted in the
  snapshot envelope, so a node restored from a snapshot can reach its peers.
