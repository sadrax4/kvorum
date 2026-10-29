# Raft implementation notes

This document walks through the parts of the `raft` package where correctness
depends on details that are easy to get wrong. It assumes familiarity with
the Raft paper; section numbers refer to it or to Ongaro's dissertation.

## Shape of the package

| File | Responsibility |
| --- | --- |
| `types.go` | Public types: `Message`, `Entry`, `HardState`, `ConfState`, `Ready`, `Config` |
| `log.go` | The log: stable storage plus the unstable tail, commit and apply cursors, conflict detection |
| `progress.go` | The leader's per-follower replication state and the in-flight window |
| `raft.go` | The state machine: elections, replication, membership, reads, transfer |
| `readonly.go` | Pending ReadIndex requests and their acknowledgements |
| `rawnode.go` | The application-facing `RawNode`: `Ready`/`Advance`, bootstrap, status |
| `sim_test.go` | The deterministic simulator |

## Terms and message handling

Every message carries the sender's term. `raft.Step` handles the term before
dispatching to the role-specific function:

- A higher term always moves the node to follower, except for Pre-Vote
  traffic, which deliberately uses a future term without committing to it.
- A lower-term `MsgApp` or `MsgHeartbeat` is answered with a `MsgAppResp`
  carrying the current term, so that a leader stranded in an old term learns
  it has been superseded. This matters with CheckQuorum, because followers
  in a lease ignore the old leader's vote requests and it would otherwise
  never advance.
- A lower-term `MsgPreVote` is rejected with the current term for the same
  reason.

## Elections: Pre-Vote and the leader lease

A follower whose election timer fires first becomes a *pre-candidate* and
asks whether it would win an election at `term+1`. Peers answer based on log
completeness and, with CheckQuorum, on whether they have heard from a leader
within an election timeout. Only after a majority says yes does the node
increment its term and run a real election.

Together these prevent the classic disruption: a node partitioned for a
while keeps a stable term, and when it reconnects it cannot force the leader
to step down. `TestPreVotePreventsTermInflation` shows the behaviour and
`TestWithoutPreVoteIsolatedNodeDisrupts` shows what happens without it.

A leadership transfer bypasses both mechanisms with `Message.Transfer`: the
leader asked for the election, so there is no disruption to prevent.

## Replication and flow control

The leader tracks each follower in one of three states:

- **probe**: one `MsgApp` in flight until the follower's log position is
  known;
- **replicate**: entries stream optimistically with up to
  `MaxInflightMsgs` messages outstanding;
- **snapshot**: a snapshot is being installed and nothing else is sent.

Rejections carry a hint. The follower reports the term of its conflicting
entry and the first index of that term; the leader skips its own entries of
that term in one step instead of decrementing by one per round trip
(`log.conflictHint` and `log.nextIndexAfterReject`). The leader also caps the
next index at the follower's last index plus one.

A heartbeat response from a follower whose window is full frees one slot, so
replication recovers even if every acknowledgement that would have freed the
window was lost.

## Commitment: the Figure 8 rule

`maybeCommit` advances the commit index to the highest index replicated on
a majority *only if the entry at that index belongs to the current term*.
Entries from earlier terms are committed indirectly when a current-term
entry above them commits. A new leader therefore appends an empty entry
immediately, which also lets it serve reads and membership changes.
`TestCommitOnlyThroughCurrentTermEntry` reproduces the paper's scenario.

A follower only ever applies entries that are both committed and stable on
its own disk (`log.stableCommitted`); applying an entry that could still be
lost in a crash would let the application acknowledge something the log does
not hold.

## Snapshots

A follower that needs an index the leader has compacted receives the
leader's latest snapshot. The follower accepts it if it is newer than its
commit index; if its log already contains the snapshot's last entry it only
moves its commit index. The application persists the snapshot before
anything after it is applied, and the leader waits for the transport's
report before resuming.

A snapshot whose membership does not include the receiving node is still
installed: that is the normal case for a node that joined after the
snapshot was taken, and the entries that follow carry its own membership
entry. The node remains unpromotable until it applies that entry.

## Membership changes

Single-server changes (ADR 0003) with two guards in `proposeConfChange`:

1. `pendingConfIndex > applied`: a previous change has not been applied.
2. `committedEntryInCurrentTerm`: the leader has not yet committed in its
   term, so its commit index might not reflect every committed change.

The configuration takes effect in `switchToConfig` when the entry is
applied. New members start with `RecentActive = true` so that CheckQuorum
does not step the leader down before they have answered once. A leader that
removes itself steps down there as well.

Before campaigning, a node checks for committed-but-unapplied membership
changes (`hasUnappliedConfChanges`): a node about to be removed must not
win an election.

## Linearizable reads

`handleReadIndex` on the leader records the commit index for the request,
acknowledges it itself, and broadcasts a heartbeat carrying the request's
context. Each heartbeat carries the newest pending context so that
acknowledgements keep flowing even if the original round was lost. When a
quorum of *voters* has acknowledged, every request queued up to that one is
released (`readOnly.advance`): an acknowledgement for a later request also
proves leadership for earlier ones.

Requests that arrive before the leader has committed in its term are held
back, and a single-voter cluster answers immediately.

Followers forward `MsgReadIndex` and receive a `MsgReadIndexResp`; the
application then waits until it has applied up to that index. Duplicate
responses are possible under message duplication, so the application
ignores contexts it does not recognise.

## Leadership transfer

`transferLeader` records the target, resets the election timer, and either
sends `MsgTimeoutNow` at once (target up to date) or sends it as soon as the
target's acknowledgement shows it has caught up. Proposals are refused in
the meantime. If the transfer does not finish within an election timeout it
is abandoned, which `TestLeaderTransfer/times_out_when_target_unreachable`
checks.

## The simulator

`sim_test.go` runs three scenarios (three nodes, five nodes with snapshots,
and a membership scenario with spare nodes) across many seeds. Each tick it
injects faults, ticks every live node, delivers due messages in shuffled
order, processes every Ready exactly as the runtime does (persist, send,
apply, snapshot), and checks:

- at most one leader per term;
- a new leader holds every entry any node has applied;
- logs that agree on an entry agree on everything before it;
- no two nodes apply different entries at an index;
- terms and commit indexes never go backwards, including across restarts
  that lose unsynced commit updates;
- confirmed read indexes cover every write applied anywhere before the read
  was requested.

After the chaos phase the network heals and the simulator requires a leader,
convergence of every member, and commitment of fresh proposals. Run a single
seed with `go test ./raft -run TestSimulation -sim.seed=N -v`; the failure
report prints the node states and the last events.
