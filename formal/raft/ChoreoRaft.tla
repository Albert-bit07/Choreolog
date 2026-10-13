------------------------------ MODULE ChoreoRaft ------------------------------
(***************************************************************************)
(* Unified asynchronous Raft-like protocol model for Choreolog (PR #9).    *)
(*                                                                         *)
(* Scope: fixed 3-server membership, no snapshots/compaction, bounded      *)
(* terms/logs/messages. This is a MODEL-CHECKED specification, not a proof *)
(* about the C++ implementation.                                           *)
(*                                                                         *)
(* Design rules:                                                           *)
(*  - State handlers read/modify STAGED state (mem). Durable state (disk)  *)
(*    changes only by PersistCompleted. One outstanding write per node.    *)
(*  - Messages that promise something about durable state are deferred in  *)
(*    pending.after and escape only on completion. Application effects     *)
(*    (ghostApplied) are produced only from disk-based Apply, via the same *)
(*    write path, so a failed write can never release a dependent effect.  *)
(*  - No artificial guards to make invariants pass. Mut* constants exist   *)
(*    only for NEGATIVE tests: each must make TLC find a counterexample.   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Servers, Values,
          MaxTerm, MaxLogLen, MaxMsgs, MaxEpoch,
          MutTruncateOnShorter,   \* PR #8 bug: truncate on a shorter prefix
          MutCommitOldTerm,       \* commit entries from earlier terms by counting
          MutVoteNotDurable,      \* forget votedFor on restart
          MutIgnoreEpoch          \* accept AE replies without epoch validation

CONSTANTS Follower, Candidate, Leader, Nil

VARIABLES
  alive,          \* explicit crash/offline state
  mem,            \* [Servers -> [term, votedFor, log]]  staged view
  disk,           \* [Servers -> [term, votedFor, log, commit, applied]]
  pending,        \* [Servers -> Nil \/ [target, after, acks]]
  role, commitIndex, nextIndex, matchIndex, votesGranted, leaderEpoch,
  net,            \* bag: [Message -> Nat]
  epochCtr,       \* ghost: next leadership-instance id
  ghostLeaders,   \* ghost: [1..MaxTerm -> SUBSET Servers]
  ghostCommitted, \* ghost: SET of [idx, entry]   (global truth)
  ghostApplied    \* ghost: SET of [node, idx, entry] released by durable Apply

nodeVars == <<mem, disk, pending, role, commitIndex, nextIndex, matchIndex,
              votesGranted, leaderEpoch>>
ghostVars == <<epochCtr, ghostLeaders, ghostCommitted, ghostApplied>>
vars == <<nodeVars, net, ghostVars, alive>>

--------------------------------------------------------------------------
\* Helpers
Quorums == {Q \in SUBSET Servers : 2 * Cardinality(Q) > Cardinality(Servers)}
IsQuorum(S) == S \in Quorums
Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b
LastTerm(l) == IF Len(l) = 0 THEN 0 ELSE l[Len(l)].term

\* ---- bag of messages
WithMessage(m, b) == IF m \in DOMAIN b THEN [b EXCEPT ![m] = b[m] + 1] ELSE b @@ (m :> 1)
WithoutMessage(m, b) ==
  IF m \in DOMAIN b
  THEN IF b[m] > 1 THEN [b EXCEPT ![m] = b[m] - 1]
       ELSE [x \in DOMAIN b \ {m} |-> b[x]]
  ELSE b
AddAll(S, b) == LET RECURSIVE F(_, _)
                    F(T, acc) == IF T = {} THEN acc
                                 ELSE LET m == CHOOSE x \in T : TRUE
                                      IN F(T \ {m}, WithMessage(m, acc))
                IN F(S, b)
TotalMsgs == LET RECURSIVE Sum(_, _)
                 Sum(D, acc) == IF D = {} THEN acc
                                ELSE LET m == CHOOSE x \in D : TRUE
                                     IN Sum(D \ {m}, acc + net[m])
             IN Sum(DOMAIN net, 0)

\* ---- log merge: truncate ONLY at the first conflicting index
ConflictIdx(l, prev, es) ==
  LET idxs == {k \in 1..Len(es) : prev + k <= Len(l) /\ l[prev + k].term # es[k].term}
  IN IF idxs = {} THEN 0 ELSE CHOOSE k \in idxs : \A j \in idxs : k <= j
Merge(l, prev, es) ==       \* precondition: prev <= Len(l), prev term matches
  IF MutTruncateOnShorter THEN SubSeq(l, 1, prev) \o es
  ELSE LET c == ConflictIdx(l, prev, es)
       IN IF c # 0 THEN SubSeq(l, 1, prev + c - 1) \o SubSeq(es, c, Len(es))
          ELSE IF prev + Len(es) > Len(l)
               THEN l \o SubSeq(es, Len(l) - prev + 1, Len(es))
               ELSE l

\* ---- durable write order: (term,votedFor) -> log -> commit -> applied.
\* A crash during a write leaves any prefix of that order, including the fully
\* written case where the completion was never observed.
Torn(old, new) ==
  LET a == [old EXCEPT !.term = new.term, !.votedFor = new.votedFor]
      b == [a EXCEPT !.log = new.log]
      c == [b EXCEPT !.commit = new.commit]
  IN {old, a, b, c, new}

--------------------------------------------------------------------------
Init ==
  /\ alive = [i \in Servers |-> TRUE]
  /\ mem = [i \in Servers |-> [term |-> 0, votedFor |-> Nil, log |-> <<>>]]
  /\ disk = [i \in Servers |-> [term |-> 0, votedFor |-> Nil, log |-> <<>>,
                                commit |-> 0, applied |-> 0]]
  /\ pending = [i \in Servers |-> Nil]
  /\ role = [i \in Servers |-> Follower]
  /\ commitIndex = [i \in Servers |-> 0]
  /\ nextIndex = [i \in Servers |-> [j \in Servers |-> 1]]
  /\ matchIndex = [i \in Servers |-> [j \in Servers |-> 0]]
  /\ votesGranted = [i \in Servers |-> {}]
  /\ leaderEpoch = [i \in Servers |-> 0]
  /\ net = <<>>   \* empty function
  /\ epochCtr = 1
  /\ ghostLeaders = [t \in 1..MaxTerm |-> {}]
  /\ ghostCommitted = {}
  /\ ghostApplied = {}

--------------------------------------------------------------------------
\* Stage a durable change: handlers see `newMem` at once; the write, the messages
\* and the application acks that depend on it wait for PersistCompleted.
StageWrite(i, newMem, msgs, acks) ==
  /\ alive[i]
  /\ pending[i] = Nil
  /\ mem' = [mem EXCEPT ![i] = newMem]
  /\ pending' = [pending EXCEPT ![i] =
       [target |-> [disk[i] EXCEPT !.term = newMem.term, !.votedFor = newMem.votedFor,
                                   !.log = newMem.log],
        after |-> msgs, acks |-> acks]]

\* ---- Persistence ---------------------------------------------------------
PersistCompleted(i) ==
  /\ alive[i]
  /\ pending[i] # Nil
  /\ disk' = [disk EXCEPT ![i] = pending[i].target]
  /\ net' = AddAll(pending[i].after, net)
  /\ ghostApplied' = ghostApplied \cup pending[i].acks
  /\ pending' = [pending EXCEPT ![i] = Nil]
  /\ UNCHANGED <<mem, role, commitIndex, nextIndex, matchIndex, votesGranted,
                 leaderEpoch, epochCtr, ghostLeaders, ghostCommitted, alive>>

\* Node restarts from disk: staged state, volatile state, deferred messages and
\* un-released acks are lost. commitIndex is rebuilt from the durable commit.
RestartTo(i, d) ==
  /\ disk' = [disk EXCEPT ![i] = d]
  /\ mem' = [mem EXCEPT ![i] = [term |-> d.term,
                                votedFor |-> IF MutVoteNotDurable THEN Nil ELSE d.votedFor,
                                log |-> d.log]]
  /\ pending' = [pending EXCEPT ![i] = Nil]
  /\ role' = [role EXCEPT ![i] = Follower]
  /\ commitIndex' = [commitIndex EXCEPT ![i] = d.commit]
  /\ nextIndex' = [nextIndex EXCEPT ![i] = [j \in Servers |-> 1]]
  /\ matchIndex' = [matchIndex EXCEPT ![i] = [j \in Servers |-> 0]]
  /\ votesGranted' = [votesGranted EXCEPT ![i] = {}]
  /\ leaderEpoch' = [leaderEpoch EXCEPT ![i] = 0]
  /\ UNCHANGED <<net, ghostVars>>

Crash(i) ==
  /\ alive[i]
  /\ \E d \in (IF pending[i] = Nil THEN {disk[i]}
                  ELSE Torn(disk[i], pending[i].target)) :
       /\ RestartTo(i, d)
       /\ alive' = [alive EXCEPT ![i] = FALSE]
PersistFailed(i) ==
  /\ alive[i] /\ pending[i] # Nil
  /\ \E d \in Torn(disk[i], pending[i].target) :
       /\ RestartTo(i, d)
       /\ alive' = [alive EXCEPT ![i] = FALSE]
Restart(i) ==
  /\ ~alive[i]
  /\ RestartTo(i, disk[i])
  /\ alive' = [alive EXCEPT ![i] = TRUE]

\* Persist learned commit knowledge (durableCommit is a LOWER BOUND of truth).
PersistCommit(i) ==
  /\ alive[i] /\ pending[i] = Nil
  /\ commitIndex[i] > disk[i].commit
  /\ pending' = [pending EXCEPT ![i] =
       [target |-> [disk[i] EXCEPT !.commit = commitIndex[i]], after |-> {}, acks |-> {}]]
  /\ UNCHANGED <<mem, disk, role, commitIndex, nextIndex, matchIndex, votesGranted,
                 leaderEpoch, net, ghostVars>>

\* Application progress is durable; the ack is released only on completion.
Apply(i) ==
  /\ alive[i] /\ pending[i] = Nil
  /\ LET n == disk[i].applied + 1 IN
     /\ n <= disk[i].commit
     /\ pending' = [pending EXCEPT ![i] =
          [target |-> [disk[i] EXCEPT !.applied = n], after |-> {},
           acks |-> {[node |-> i, idx |-> n, entry |-> disk[i].log[n]]}]]
  /\ UNCHANGED <<mem, disk, role, commitIndex, nextIndex, matchIndex, votesGranted,
                 leaderEpoch, net, ghostVars>>

--------------------------------------------------------------------------
\* ---- Terms ---------------------------------------------------------------
UpdateTerm(i, m) ==
  /\ alive[i]
  /\ m \in DOMAIN net
  /\ m.mdst = i
  /\ m.mterm > mem[i].term
  /\ StageWrite(i, [term |-> m.mterm, votedFor |-> Nil, log |-> mem[i].log], {}, {})
  /\ role' = [role EXCEPT ![i] = Follower]
  /\ votesGranted' = [votesGranted EXCEPT ![i] = {}]
  /\ UNCHANGED <<disk, commitIndex, nextIndex, matchIndex, leaderEpoch, net, ghostVars>>

\* ---- Elections -------------------------------------------------------------
Timeout(i) ==
  /\ alive[i]
  /\ role[i] \in {Follower, Candidate}
  /\ mem[i].term < MaxTerm
  /\ LET t == mem[i].term + 1
         req(j) == [mtype |-> "RVReq", msrc |-> i, mdst |-> j, mterm |-> t,
                    mlastTerm |-> LastTerm(mem[i].log), mlastIdx |-> Len(mem[i].log)]
     IN StageWrite(i, [term |-> t, votedFor |-> i, log |-> mem[i].log],
                   {req(j) : j \in Servers \ {i}}, {})
  /\ role' = [role EXCEPT ![i] = Candidate]
  /\ votesGranted' = [votesGranted EXCEPT ![i] = {i}]
  /\ UNCHANGED <<disk, commitIndex, nextIndex, matchIndex, leaderEpoch, net, ghostVars>>

HandleRVReq(i, m) ==
  /\ alive[i]
  /\ m \in DOMAIN net /\ m.mtype = "RVReq" /\ m.mdst = i
  /\ m.mterm <= mem[i].term
  /\ pending[i] = Nil                  \* everything read here is durable
  /\ LET upToDate == \/ m.mlastTerm > LastTerm(mem[i].log)
                     \/ /\ m.mlastTerm = LastTerm(mem[i].log)
                        /\ m.mlastIdx >= Len(mem[i].log)
         canVote == m.mterm = mem[i].term /\ mem[i].votedFor \in {Nil, m.msrc}
         grant == canVote /\ upToDate
         resp == [mtype |-> "RVResp", msrc |-> i, mdst |-> m.msrc,
                  mterm |-> mem[i].term, granted |-> grant]
     IN IF grant /\ mem[i].votedFor = Nil
        THEN \* a new vote is a durable change: reply only after it is on disk
             /\ StageWrite(i, [mem[i] EXCEPT !.votedFor = m.msrc], {resp}, {})
             /\ net' = WithoutMessage(m, net)
        ELSE /\ net' = WithMessage(resp, WithoutMessage(m, net))
             /\ UNCHANGED <<mem, pending>>
  /\ UNCHANGED <<disk, role, commitIndex, nextIndex, matchIndex, votesGranted,
                 leaderEpoch, ghostVars>>

HandleRVResp(i, m) ==
  /\ alive[i]
  /\ m \in DOMAIN net /\ m.mtype = "RVResp" /\ m.mdst = i
  /\ m.mterm <= mem[i].term
  /\ net' = WithoutMessage(m, net)
  /\ IF role[i] = Candidate /\ m.mterm = mem[i].term /\ m.granted
     THEN votesGranted' = [votesGranted EXCEPT ![i] = votesGranted[i] \cup {m.msrc}]
     ELSE UNCHANGED votesGranted
  /\ UNCHANGED <<mem, disk, pending, role, commitIndex, nextIndex, matchIndex,
                 leaderEpoch, ghostVars>>

BecomeLeader(i) ==
  /\ alive[i]
  /\ role[i] = Candidate
  /\ pending[i] = Nil                  \* the self-vote must be durable
  /\ IsQuorum(votesGranted[i])
  /\ epochCtr <= MaxEpoch
  /\ role' = [role EXCEPT ![i] = Leader]
  /\ leaderEpoch' = [leaderEpoch EXCEPT ![i] = epochCtr]
  /\ epochCtr' = epochCtr + 1
  /\ nextIndex' = [nextIndex EXCEPT ![i] = [j \in Servers |-> Len(mem[i].log) + 1]]
  /\ matchIndex' = [matchIndex EXCEPT ![i] = [j \in Servers |-> 0]]
  /\ ghostLeaders' = [ghostLeaders EXCEPT ![mem[i].term] = @ \cup {i}]
  /\ UNCHANGED <<mem, disk, pending, commitIndex, votesGranted, net,
                 ghostCommitted, ghostApplied>>

\* ---- Replication -----------------------------------------------------------
ClientPropose(i, v) ==
  /\ alive[i]
  /\ role[i] = Leader
  /\ Len(mem[i].log) < MaxLogLen
  /\ StageWrite(i, [mem[i] EXCEPT !.log = Append(@, [term |-> mem[i].term, val |-> v])],
                {}, {})
  /\ UNCHANGED <<disk, role, commitIndex, nextIndex, matchIndex, votesGranted,
                 leaderEpoch, net, ghostVars>>

SendAppend(i, j) ==
  /\ alive[i]
  /\ role[i] = Leader /\ i # j
  /\ \E k \in {0, 1} :                       \* heartbeat or one entry
       LET prev == nextIndex[i][j] - 1
           es == IF k = 1 /\ Len(mem[i].log) >= nextIndex[i][j]
                 THEN <<mem[i].log[nextIndex[i][j]]>> ELSE <<>>
       IN /\ prev <= Len(mem[i].log)
          /\ net' = WithMessage([mtype |-> "AEReq", msrc |-> i, mdst |-> j,
                                 mterm |-> mem[i].term, mepoch |-> leaderEpoch[i],
                                 mprev |-> prev,
                                 mprevTerm |-> IF prev = 0 THEN 0 ELSE mem[i].log[prev].term,
                                 mentries |-> es, mcommit |-> commitIndex[i]], net)
  /\ UNCHANGED <<nodeVars, ghostVars>>

HandleAEReq(i, m) ==
  /\ alive[i]
  /\ m \in DOMAIN net /\ m.mtype = "AEReq" /\ m.mdst = i
  /\ m.mterm <= mem[i].term
  /\ \/ \* stale leader: refuse (promises nothing durable)
        /\ m.mterm < mem[i].term
        /\ net' = WithMessage([mtype |-> "AEResp", msrc |-> i, mdst |-> m.msrc,
                               mterm |-> mem[i].term, mepoch |-> m.mepoch, msuccess |-> FALSE,
                               mprev |-> m.mprev, mcount |-> 0, mhint |-> 0],
                              WithoutMessage(m, net))
        /\ UNCHANGED <<nodeVars, ghostVars>>
     \/ /\ m.mterm = mem[i].term
        /\ pending[i] = Nil
        /\ LET l == mem[i].log
               prevOk == m.mprev = 0 \/ (m.mprev <= Len(l) /\ l[m.mprev].term = m.mprevTerm)
               newLog == Merge(l, m.mprev, m.mentries)
               lastNew == m.mprev + Len(m.mentries)
               ok == [mtype |-> "AEResp", msrc |-> i, mdst |-> m.msrc, mterm |-> m.mterm,
                      mepoch |-> m.mepoch, msuccess |-> TRUE, mprev |-> m.mprev,
                      mcount |-> Len(m.mentries), mhint |-> 0]
               bad == [mtype |-> "AEResp", msrc |-> i, mdst |-> m.msrc, mterm |-> m.mterm,
                       mepoch |-> m.mepoch, msuccess |-> FALSE, mprev |-> m.mprev,
                       mcount |-> 0,
                       mhint |-> IF m.mprev > Len(l) THEN Len(l) ELSE m.mprev - 1]
           IN IF ~prevOk
              THEN /\ net' = WithMessage(bad, WithoutMessage(m, net))
                   /\ role' = [role EXCEPT ![i] = Follower]
                   /\ UNCHANGED <<mem, disk, pending, commitIndex, nextIndex, matchIndex,
                                  votesGranted, leaderEpoch, ghostVars>>
              ELSE /\ role' = [role EXCEPT ![i] = Follower]
                   /\ commitIndex' = [commitIndex EXCEPT ![i] =
                         Max(@, Min(m.mcommit, Min(lastNew, Len(newLog))))]
                   /\ IF newLog = l
                      THEN /\ net' = WithMessage(ok, WithoutMessage(m, net))
                           /\ UNCHANGED <<mem, pending>>
                      ELSE /\ StageWrite(i, [mem[i] EXCEPT !.log = newLog], {ok}, {})
                           /\ net' = WithoutMessage(m, net)
                   /\ UNCHANGED <<disk, nextIndex, matchIndex, votesGranted, leaderEpoch,
                                  ghostVars>>

HandleAEResp(i, m) ==
  /\ alive[i]
  /\ m \in DOMAIN net /\ m.mtype = "AEResp" /\ m.mdst = i
  /\ m.mterm <= mem[i].term
  /\ net' = WithoutMessage(m, net)
  /\ LET j == m.msrc
         current == /\ role[i] = Leader /\ m.mterm = mem[i].term
                    /\ (MutIgnoreEpoch \/ m.mepoch = leaderEpoch[i])
         newMatch == m.mprev + m.mcount
     IN IF ~current
        THEN UNCHANGED <<matchIndex, nextIndex>>
        ELSE IF m.msuccess
             THEN /\ matchIndex' = [matchIndex EXCEPT ![i][j] = Max(@, newMatch)]
                  /\ nextIndex' = [nextIndex EXCEPT ![i][j] = Max(@, newMatch + 1)]
             ELSE IF m.mprev + 1 = nextIndex[i][j]       \* answers the current probe
                  THEN /\ nextIndex' = [nextIndex EXCEPT ![i][j] =
                              Max(matchIndex[i][j] + 1, Min(@ - 1, m.mhint + 1))]
                       /\ UNCHANGED matchIndex
                  ELSE UNCHANGED <<matchIndex, nextIndex>>
  /\ UNCHANGED <<mem, disk, pending, role, commitIndex, votesGranted, leaderEpoch,
                 ghostVars>>

\* Self counts toward the quorum only for entries that are durable.
SelfDurable(i, n) ==
  /\ Len(disk[i].log) >= n /\ disk[i].log[n] = mem[i].log[n]

AdvanceCommit(i) ==
  /\ alive[i]
  /\ role[i] = Leader
  /\ \E n \in (commitIndex[i] + 1)..Len(mem[i].log) :
       /\ \/ MutCommitOldTerm
          \/ mem[i].log[n].term = mem[i].term
       /\ IsQuorum({j \in Servers \ {i} : matchIndex[i][j] >= n} \cup
                   IF SelfDurable(i, n) THEN {i} ELSE {})
       /\ commitIndex' = [commitIndex EXCEPT ![i] = n]
       /\ ghostCommitted' = ghostCommitted \cup
            {[idx |-> k, entry |-> mem[i].log[k]] : k \in 1..n}
  /\ UNCHANGED <<mem, disk, pending, role, nextIndex, matchIndex, votesGranted,
                 leaderEpoch, net, epochCtr, ghostLeaders, ghostApplied>>

\* ---- Network faults --------------------------------------------------------
DropMsg(m) ==
  /\ m \in DOMAIN net
  /\ net' = WithoutMessage(m, net)
  /\ UNCHANGED <<nodeVars, ghostVars>>
DuplicateMsg(m) ==
  /\ m \in DOMAIN net
  /\ net' = WithMessage(m, net)
  /\ UNCHANGED <<nodeVars, ghostVars>>

--------------------------------------------------------------------------
ActiveNext ==
  \/ \E i \in Servers : Timeout(i) \/ BecomeLeader(i) \/ PersistCompleted(i)
        \/ PersistCommit(i) \/ Apply(i) \/ AdvanceCommit(i)
  \/ \E i \in Servers, v \in Values : ClientPropose(i, v)
  \/ \E i, j \in Servers : SendAppend(i, j)
  \/ \E i \in Servers, m \in DOMAIN net :
        UpdateTerm(i, m) \/ HandleRVReq(i, m) \/ HandleRVResp(i, m)
        \/ HandleAEReq(i, m) \/ HandleAEResp(i, m)
  \/ \E m \in DOMAIN net : DropMsg(m) \/ DuplicateMsg(m)

\* Core-safety profile: a finite, fault-free execution with one outstanding
\* AppendEntries exchange per peer and no redundant heartbeats. It deliberately
\* retains term changes, durable writes, real vote messages, replication and
\* commitment. It does NOT cover crashes, partitions, retransmissions or
\* arbitrary message duplication: those remain in full Spec and separate tests.
CoreSendAppend(i, j) ==
  /\ i # j
  /\ nextIndex[i][j] <= Len(mem[i].log)
  /\ \A m \in DOMAIN net :
       ~((m.mtype = "AEReq" /\ m.msrc = i /\ m.mdst = j)
          \/ (m.mtype = "AEResp" /\ m.msrc = j /\ m.mdst = i))
  /\ SendAppend(i, j)

CoreNext ==
  \/ \E i \in Servers :
       Timeout(i) \/ BecomeLeader(i) \/ PersistCompleted(i)
       \/ PersistCommit(i) \/ Apply(i) \/ AdvanceCommit(i)
  \/ \E i \in Servers, v \in Values : ClientPropose(i, v)
  \/ \E i, j \in Servers : CoreSendAppend(i, j)
  \/ \E i \in Servers, m \in DOMAIN net :
       UpdateTerm(i, m) \/ HandleRVReq(i, m) \/ HandleRVResp(i, m)
       \/ HandleAEReq(i, m) \/ HandleAEResp(i, m)

CoreSpec == Init /\ [][CoreNext]_vars

Next ==
  \/ /\ UNCHANGED alive /\ ActiveNext
  \/ \E i \in Servers : Crash(i) \/ PersistFailed(i) \/ Restart(i)

Spec == Init /\ [][Next]_vars

StateConstraint == TotalMsgs <= MaxMsgs
ServerSymmetry == Permutations(Servers)

--------------------------------------------------------------------------
\* ============================ INVARIANTS ================================
TypeOK ==
  /\ alive \in [Servers -> BOOLEAN]
  /\ \A i \in Servers : mem[i].term \in 0..MaxTerm /\ disk[i].term \in 0..MaxTerm
  /\ \A i \in Servers : commitIndex[i] <= Len(mem[i].log)

\* Staged and durable agree whenever no write is in flight.
PendingSynced ==
  \A i \in Servers : pending[i] = Nil =>
    /\ mem[i].term = disk[i].term /\ mem[i].log = disk[i].log
    /\ (MutVoteNotDurable \/ mem[i].votedFor = disk[i].votedFor)

ElectionSafety == \A t \in 1..MaxTerm : Cardinality(ghostLeaders[t]) <= 1

LogMatching ==
  \A i, j \in Servers : \A k \in 1..Min(Len(mem[i].log), Len(mem[j].log)) :
    mem[i].log[k].term = mem[j].log[k].term =>
      \A x \in 1..k : mem[i].log[x] = mem[j].log[x]

CommittedUnique ==
  \A a, b \in ghostCommitted : a.idx = b.idx => a.entry = b.entry

\* Every leader of a later term holds every committed entry.
LeaderCompleteness ==
  \A c \in ghostCommitted : \A s \in Servers :
    (role[s] = Leader /\ mem[s].term > c.entry.term) =>
      (c.idx <= Len(mem[s].log) /\ mem[s].log[c.idx] = c.entry)

\* A commit is durable on a quorum and stays that way.
CommittedNeverLost ==
  \A c \in ghostCommitted : \E Q \in Quorums : \A s \in Q :
    c.idx <= Len(disk[s].log) /\ disk[s].log[c.idx] = c.entry

\* Local knowledge never exceeds global truth.
CommitKnowledgeSound ==
  \A i \in Servers :
    /\ \A k \in 1..commitIndex[i] : [idx |-> k, entry |-> mem[i].log[k]] \in ghostCommitted
    /\ disk[i].applied <= disk[i].commit /\ disk[i].commit <= Len(disk[i].log)
    /\ \A k \in 1..disk[i].commit : [idx |-> k, entry |-> disk[i].log[k]] \in ghostCommitted

StateMachineSafety ==
  \A a \in ghostApplied :
    /\ [idx |-> a.idx, entry |-> a.entry] \in ghostCommitted
    /\ \A b \in ghostApplied : a.idx = b.idx => a.entry = b.entry

\* A leader's matchIndex for j never exceeds what j durably holds, unless j has
\* moved to a later term (a stale-epoch ack must not inflate it).
MatchIndexSound ==
  \A i \in Servers : role[i] = Leader =>
    \A j \in Servers \ {i} :
      disk[j].term > mem[i].term \/
      /\ matchIndex[i][j] <= Len(disk[j].log)
      /\ matchIndex[i][j] <= Len(mem[i].log)
      /\ SubSeq(disk[j].log, 1, matchIndex[i][j]) =
         SubSeq(mem[i].log, 1, matchIndex[i][j])

SafetyInvariant ==
  /\ TypeOK /\ PendingSynced /\ ElectionSafety /\ LogMatching /\ CommittedUnique
  /\ LeaderCompleteness /\ CommittedNeverLost /\ CommitKnowledgeSound
  /\ StateMachineSafety /\ MatchIndexSound

\* Reachability checks: use *separate* TLC configurations that invert each
\* predicate and intentionally expect a concrete invariant counterexample.
\* A successful reachability test means TLC FOUND the witness, not that the
\* negated predicate holds universally.
SomeLeader == \E i \in Servers : role[i] = Leader
SomeReplicatedEntry ==
  \E i, j \in Servers :
    i # j /\ Len(disk[i].log) > 0
    /\ disk[i].log = disk[j].log
SomeCommittedEntry == ghostCommitted # {}
SomeAppliedEntry == ghostApplied # {}
NoLeaderWitness == ~SomeLeader
NoReplicationWitness == ~SomeReplicatedEntry
NoCommitWitness == ~SomeCommittedEntry
NoApplicationWitness == ~SomeAppliedEntry

\* ============================ ACTION PROPERTIES ============================
\* Recovery and every other step never roll back durable progress.
DurableMonotone ==
  [][\A i \in Servers : /\ disk'[i].term >= disk[i].term
                        /\ disk'[i].commit >= disk[i].commit
                        /\ disk'[i].applied >= disk[i].applied
                        /\ Len(disk'[i].log) >= disk[i].commit]_vars
AppliedNeverShrinks == [][ghostApplied \subseteq ghostApplied']_vars
==========================================================================