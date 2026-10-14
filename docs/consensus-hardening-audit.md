# PR #14 consensus hardening audit

This ledger distinguishes concrete defects fixed in this PR from architectural
work that remains open. A passing regression suite is not a proof of full Raft,
cryptographic authentication or arbitrary power-loss safety.

## Concrete defects addressed

| Finding | Correction / regression evidence |
| --- | --- |
| Metadata read errors reset term/vote | Checked metadata result; replica recovery rejects corrupt metadata. |
| Continued operation after ambiguous term/vote, WAL or commit writes | Production fail-stop state blocks sends, campaigns and client writes until disk recovery. |
| Sender/session mismatch and non-member votes | Bind claimed sender to established session ID; reject non-members and session reassignment. This is not authentication. |
| Commit state/receipts exposed before metadata succeeds | Stage commit batches; persist metadata before publishing application state. Failed-submit and failed-commit tests inspect state and receipts. |
| Invalid commit batch exposes its valid prefix | Reject the whole batch without changing application state. |
| Submission/recovery lowers a persisted election term or drops its vote | Preserve higher term and vote while advancing the commit index. |
| Follower commits an unmatched suffix | Bound commit by the prefix matched by this AppendEntries request. |
| Append envelopes contain gaps, decreasing terms or future terms | Reject invalid envelopes before changing the WAL or term. |
| Snapshot install continues after partial storage failure | Validate peer input first; halt on subsequent install failures. Test metadata and WAL replacement failures and real reopen. |
| Stale or oversized append acknowledgements supply quorum | Match frame ID, request term and exact covered index; delayed failures cannot rewind a newer request. |
| Snapshot acknowledgements skip chunks or complete an unrelated transfer | Match frame ID, term and exact chunk end offset. |
| Correlation tracking prevents progress when RTT exceeds a heartbeat | Retransmit the same outstanding frame/ID; delayed-response simulation must converge. |
| Read-only WAL projection accepts decreasing entry terms | Validate the complete log, including uncommitted entries; retain valid equal/increasing terms. |

`ConsensusHardeningTest` and `RaftDomainBridgeTest` exercise these boundaries.
The ordinary suite also covers bounded catch-up, snapshots, fault simulation,
TCP failover and the experimental transition trace contract. Sanitizer CI omits
the two large CatchUpTest cases; those remain required in the normal suite.
Windows, management API and remote CI results must be checked on the pushed head.

## Still open; do not report these as solved

- Optional [peer mTLS](peer-tls.md) now authenticates configured peer identities
  when enabled on every member. Plaintext mode remains unauthenticated.
  Automated issuance, live reload and immediate certificate revocation remain
  open; the initial lifecycle uses overlapping trust and rolling restarts.
- Snapshot installation now has a durable redo transaction spanning WAL,
  metadata and snapshot replacement; see `docs/persistence.md`. General
  multi-operation transactions and exhaustive power-loss testing remain open.
- Machine-checked refinement from TLA+ to the deployed production Replica.
  Bounded model results and the experimental trace checker have separate scope.
- Production migration to the experimental transition core and snapshot-offset
  mapping in the read-only WAL projection.

PR #14 must not be described as closing the entire consensus/security program.
