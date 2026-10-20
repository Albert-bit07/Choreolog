# Consensus field and persistence inventory

Reviewed against `332b11b681a6f2f5d007daa25ba67f0b127e5280` (PR #24; runtime unchanged from PR #23).
This is a source audit and regression map, not a refinement proof. The
transition backend is selectable; legacy remains the default. Both use
FileEngine. The isolated `DurableNode` checkpoint driver is test infrastructure,
not the node's production storage adapter.

## Pure core

Declarations: [transition.hpp](../cpp-runtime/include/choreoos/raft/transition.hpp).
Mutation rules: [transition.cpp](../cpp-runtime/src/raft/transition.cpp).

| Fields | Ownership and recovery | Safety obligation |
|---|---|---|
| `Config.self`, `peers` | Immutable static membership; host validates distinct IDs | Quorum and sender membership use the same configuration; no joint consensus |
| `Durable.term`, `voted_for` | Acknowledged disk state; recovered from metadata | Persist before vote/term-dependent reply; never vote twice in a term |
| `Durable.log` (`Entry.term`, `command`) | Shared immutable retained suffix; recovered from WAL | Preserve committed prefix; exact canonical command/index/term mapping |
| `Durable.commit`, `applied` | Absolute indexes; host reconstructs application from committed replay | `snapshot_index <= applied <= commit <= last_index`; physical application at commit, core applied is confirmation |
| `Durable.snapshot_index`, `snapshot_term` | Verified snapshot boundary, with application and receipts owned by host | Preserve anchor term and only an agreeing suffix |
| `Volatile.role`, `votes`, `leadership_epoch` | Discarded on restart | Distinct configured voters; old leadership progress grants no new authority |
| `Volatile.peers`: `next`, `match` | Volatile per-peer replication progress | Match derives from exact acknowledged coverage, never follower hint |
| `PeerProgress.in_flight`: `Probe.request_id`, `term`, `prev`, `count`, `request` | Exact outstanding append retained across retries | Delayed/duplicate replies cannot acknowledge another probe |
| `PeerProgress.snapshot_in_flight`: peer/index/term/request ID | Volatile root snapshot probe | Only correlated final installation advances match |
| `Volatile.read`: context/term/index, acknowledgements, probes | One volatile round; dropped on restart/step-down | Fresh distinct quorum and applied captured prefix; no clock lease |
| `Node.pending`: token/target/after/apply_after/self_vote/snapshot_after | Staged write, not disk truth | Release dependent effects only for matching successful completion |
| `Node.next_token`, `next_request_id` | Volatile correlation counters | Stale callbacks must not release effects; restart drops old outstanding work |
| `Node.halted` | Fail-stop latch | No progress after uncertainty; production host must reopen actual disk |
| `PersistRequest.kind` | State update, compaction or install intent | Select the matching physical operation, then verify its result |

Every input field is part of the audit boundary: vote term/candidate/last
index/last term/request ID; append term/leader/previous index and term/entries/
leader commit/request ID; reply term/member/success/hint/matched/request ID;
snapshot term/leader/index/snapshot term/request ID/readiness; read contexts;
persistence tokens; proposed command. Timers request transitions but convey no
quorum authority. `Effects` contains intents, never evidence of completed I/O.

## Production host and transport facade

| Owner | Fields that influence authoritative behavior | Recovery/ordering |
|---|---|---|
| `RuntimeHost` | `config_`, `node_`, `adapter_`, `snapshot_every_`, `pinned_`, `published_`, `lease_` | One owner strand; drain and verify storage/application before returning effects; pin snapshot generation; reopen after error |
| `FileEngineAdapter` | `directory_`, `store_`, `current_`, `view_`, `uncertain_`; gated `test_mode_`/`fault_` | Delta validates boundary/common prefix, then metadata, truncate, append, commit; invalidate cached projection; failure requires recovery |
| `CoreReplica` | `config_`, `host_`, `leader_id_`, `rng_`, `elapsed_`, `timeout_`, `snapshot_correlation_` | Timers outside pure core; append current-term no-op on leadership; hints are not read authority |
| `CoreReplica` | `speculative_`, `speculative_dirty_`, `index_initialized_`, `command_index_` | Proposal validation/cache only; client success still requires actual durable receipt |
| `CoreReplica` | `outgoing_` pinned snapshot/frame/end/final; `incoming_` leader/term/index/snapshot term/hash/bytes; `failure_` | Bounded exact chunks; verify whole payload; failure blocks success; partial streams never applied |
| `NodeServer` / `RuntimeReplica` | Backend selector, authenticated peer binding, timer callbacks, bounded client waiters/correlations/deadlines, commit callback | Owner strand serializes access; only durable committed receipts complete writes; local status/state are not linearizable |

`NodeServer::Impl` owns `config`, `replica`, TLS context, `io`/`strand`,
client/peer acceptors, heartbeat/reconnect timers and bound port. It also owns
`queue_budget`, sessions, `dialing`, fault controls `blocked_peers`,
`delayed_peers`/`delayed_sends`/`paused`, and cached hash/state. Each pending
write stores command ID, optional event, duplicate flag, session, correlation
and independent deadline timer. Session peer IDs (`peer_id`, `expected_peer`,
`verified_peer`) bind protocol origin; framing buffers, read/write deadlines
and byte reservations bound transport work. These fields can delay/refuse
progress but cannot grant commitment. Test fault controls remain gated.

Sender callbacks and commit hooks release already authorized effects; counters
and status strings are observations, not sources of term/log/commit truth.
Directory ownership, sync mode, chunk bounds and test-mode flags remain
correctness-relevant configuration, even though they are outside `step`.

## Legacy backend

Declarations: [replica.hpp](../cpp-runtime/include/choreoos/replication/replica.hpp).

| Fields | Role |
|---|---|
| `config_` (ID, leader, peers, elections, directory, seed, timing, snapshot and batch bounds) | Static membership/mode and operational bounds; fixed-leader mode is not consensus |
| `store_`, `current_term_`, `voted_for_` | Durable truth and cached term/vote; persist before dependent publication |
| `role_`, `votes_from_`, `peers_` | Volatile role, distinct votes; each peer owns next/match, snapshot index/offset/sent tick and exact `probe` frame/term/index/offset |
| `speculative_`, `speculative_dirty_`, `history_anchor_`, `command_index_` | Derived proposal/history caches; never replace committed state |
| `incoming_snapshot_` active/index/term/hash/data | Partial transfer; no publication before complete validation and durable install |
| `ticks_`, `next_correlation_`, `election_elapsed_`, `election_timeout_`, `rng_` | Timing/retry context; no safety claim based on elapsed time |
| `halted_` | Fail-stop until reopen; sender/hook/metrics do not override it |

## Disk/application state

`FileEngine` owns directory/options/mutex/WAL, Engine, recovery report,
commit index, snapshot and `recovery_required_`. Test faults are gated and
one-shot. `NodeMetadata` stores term, commit index and optional vote together.
The WAL owns ordered events and record ends; checksums and contiguous indexes
are validated at recovery. Stats are observational.

`ChoreographyState` holds domain tables, last_applied/event term, full-event
history aliases and compact receipts `(index, term, fingerprint)` keyed by
command ID. Speculative copies do not authorize replies. Snapshot envelope
index/term/hash plus payload must agree. The snapshot payload includes receipts;
the domain hash alone does not establish receipt equivalence. Current receipt
fingerprints are **64-bit FNV-1a**, not SHA-256 or authentication. Receipts and
domain tables remain unbounded. Snapshot transaction decision contains all
three replacement images (snapshot/metadata/WAL); recovery resolves it first.

## Persist-before-reply edges

Test names below are executable GoogleTest filters in the ordinary CTest suite.
Paths are under `cpp-runtime/tests/unit/` unless stated otherwise.

| Edge | Physical ordering / failure behavior | Regression evidence |
|---|---|---|
| Campaign/self-vote | Metadata term+self vote before RequestVote and counted self vote | `RaftTransitionTest.ElectionDoesNotEmitVotesBeforeDurableSelfVote`; `RaftDurableTest.SelfVoteAndGrantedVoteSurviveRealDiskReopen` |
| Granted vote / higher term | Metadata sync/replace/directory sync before positive reply; error halts | `RuntimeHostTest.FailedMetadataPublishesNoVoteAndRequiresRealDiskRecovery`; `ConsensusHardeningTest.HigherTermWriteFailureHaltsAndPreventsAcknowledgements` |
| Append / suffix repair | Validate; persist term, truncate only uncommitted suffix, sync new WAL; then exact ack | `RaftTransitionTest.ReplicationAndApplicationEffectsFollowDurability`; `RuntimeHostTest.AmbiguousAppendNeverConfirmsCommitAndRecoveryRetainsUncommittedTail` |
| Commit / client result | Validate entire batch, reserve publication, persist commit metadata, publish state+receipts, then complete client | `ConsensusHardeningTest.FailedCommitPublishesNeitherStateNorReceipts`; `RuntimeHostTest.MajorityAckCommitsOnlyTheSentEntryAfterActualDiskPersistence` |
| Apply confirmation | Verify FileEngine already applied commit before releasing core application effects; never apply twice | `RuntimeHostTest.SingleNodeCommitConfirmsDurableApplicationExactlyOnceAndReopens` |
| Snapshot creation/compaction | Save verified committed application/receipts before WAL prefix removal; core boundary waits for completion | `RuntimeHostTest.AutomaticMaintenanceChecksActualApplicationReceiptsAndBoundary`; `RaftLocalCompactionTest` suite |
| Snapshot install | Sync redo decision, replace snapshot/metadata/WAL, retire journal, verify host projection, then final success | `SnapshotTransactionTest` suite; `RuntimeHostTest.SnapshotNeedsVerifiedPayloadAndCompletesTermAndApplicationBeforeReply` |
| Failure / callback | Stale completion does nothing; failed matching write halts, drops effects; recovery derives disk truth | `RaftTransitionTest.StalePersistenceTokenCannotReleaseDependentEffects`; `RaftDurableTest.FailureAfterReplaceIsAmbiguousButRecoveryUsesDisk` |
| Read completion | No read-only persistence; fresh quorum plus applied captured commit before Ready | `RaftReadBarrierTest` suite; `RuntimeHostTest.ReadResultCoversRealApplicationAndCompactionWithoutWriting` |

Sync/rename/filesystem semantics are assumptions, not a simulated proof of
hardware power-loss safety. Buffered benchmarks do not establish durability.

## Explicit deviations and executable boundaries

| Boundary from full production Raft | Executable regression or refusal | Remaining work |
|---|---|---|
| Fixed leader with elections off; legacy is rollout default | `ConfigTest` and `TransitionRuntimeConfigTest.InvalidProgrammaticSelectionDoesNotTouchTheStore` | Dual-backend parity/soak/benchmark gate before default flip |
| One-entry transition append, finite retained window | `RaftTransitionTest.RetainedWindowReturnsExplicitBackpressureWithoutHalting` | Profile before pipelining; capacity is backpressure, not dropped history |
| No public quorum read yet | `RaftReadBarrierTest.IsolatedLeaderCannotCompleteAndCancellationBoundsState`; public local read coverage in `ClusterTest.ManagementReadsCommittedEventsAndRejectsProductionFaults` | TCP/gRPC partitioned-old-leader tests and public API integration |
| No pre-vote, membership changes or leadership transfer | `RaftTransitionTest.NonMemberMessagesCannotChangeTermOrLog` | New specification before adding features |
| Snapshot/read actions absent from v1 trace/model relation | `RaftSnapshotInstallTest.SnapshotActionsRemainOutsideV1TraceEvenWithoutOffsets`; `RaftReadBarrierTest.V1TraceRefusesReadActionsAndActiveReadState` | Trace v2, model transitions, TLC replay and justified stuttering |
| Persisted application reconstructed from commit, not separate per-event applied file | `RaftRecoveryProjectionTest` and `RuntimeHostTest` suites | Model/host correspondence remains hand reviewed |
| Partial file/fault coverage, not exhaustive lost-write emulation | `DurabilityTest.TornTailOfEveryPartialLengthIsDiscarded`; `SnapshotTransactionTest` suite | T1-E write-boundary power-loss harness |

A missing feature is not made correct by testing its absence. These regressions
freeze current boundaries; they do not discharge the remaining roadmap gates.
