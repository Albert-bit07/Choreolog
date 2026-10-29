------------------ MODULE ChoreoRaftDualPersistenceFaults ------------------
\* Restricted, action-ordered two-node persistence-failure witness.
\* The pinned ChoreoRaft.tla model remains unchanged.
\*
\* Scope: one committed term-one entry; a successor's failed term-two
\* self-vote write (with the old durable record retained), restart/retry;
\* then the former leader's failed term-two grant write (with its
\* previously fsynced term-only record retained); a third peer grants
\* the retrying candidate a durable vote. This is one SELECTED Torn
\* outcome at each failure, not a claim of atomic physical disk writes.
\*
\* The C++ transition core uses a combined term/vote persistence callback.
\* Here UpdateTerm, term fsync, HandleRVReq, and vote fsync remain separate
\* model actions. No lockstep implementation refinement is asserted.
EXTENDS ChoreoRaft

VARIABLE faultPhase
dualVars == <<vars, faultPhase>>

DualFaultInit ==
  /\ Init
  /\ faultPhase = 0

\* Before the first failure, reuse the real bounded handoff actions.
\* Once the successor stages its term-two self-vote, do not permit an
\* ordinary successful completion to bypass the required failure.
BeforeCandidateFailure ==
  /\ faultPhase = 0
  /\ (IF pending[CombinedSuccessor] = Nil THEN TRUE
      ELSE ~(role[CombinedSuccessor] = Candidate
             /\ pending[CombinedSuccessor].target.term = 2
             /\ pending[CombinedSuccessor].target.votedFor = CombinedSuccessor))
  /\ CombinedNext
  /\ UNCHANGED faultPhase

FailCandidateSelfVote ==
  /\ faultPhase = 0
  /\ alive[CombinedSuccessor]
  /\ role[CombinedSuccessor] = Candidate
  /\ pending[CombinedSuccessor] # Nil
  /\ pending[CombinedSuccessor].target.term = 2
  /\ pending[CombinedSuccessor].target.votedFor = CombinedSuccessor
  /\ disk[CombinedSuccessor].term = 1
  /\ PersistFailed(CombinedSuccessor)
  /\ disk'[CombinedSuccessor] = disk[CombinedSuccessor]
  /\ faultPhase' = 1

RestartCandidate ==
  /\ faultPhase = 1
  /\ Restart(CombinedSuccessor)
  /\ faultPhase' = 2

RetryCandidateElection ==
  /\ faultPhase = 2
  /\ Timeout(CombinedSuccessor)
  /\ faultPhase' = 3

FsyncRetriedSelfVote ==
  /\ faultPhase = 3
  /\ pending[CombinedSuccessor] # Nil
  /\ pending[CombinedSuccessor].target.term = 2
  /\ pending[CombinedSuccessor].target.votedFor = CombinedSuccessor
  /\ PersistCompleted(CombinedSuccessor)
  /\ faultPhase' = 4

\* The old leader must first fsync the higher term with Nil vote, then
\* stage the new grant. A successful grant fsync is not allowed here.
BeforeFollowerFailure ==
  /\ faultPhase = 4
  /\ (IF pending[CombinedPrimary] = Nil THEN TRUE
      ELSE ~(pending[CombinedPrimary].target.term = 2
             /\ pending[CombinedPrimary].target.votedFor = CombinedSuccessor))
  /\ CombinedNext
  /\ UNCHANGED faultPhase

FailFollowerGrant ==
  /\ faultPhase = 4
  /\ alive[CombinedPrimary]
  /\ pending[CombinedPrimary] # Nil
  /\ pending[CombinedPrimary].target.term = 2
  /\ pending[CombinedPrimary].target.votedFor = CombinedSuccessor
  /\ disk[CombinedPrimary].term = 2
  /\ disk[CombinedPrimary].votedFor = Nil
  /\ PersistFailed(CombinedPrimary)
  /\ disk'[CombinedPrimary] = disk[CombinedPrimary]
  /\ faultPhase' = 5

\* Only after both failed writes do we let the third peer process the
\* successor's real RVReq. Its term and vote are separately fsynced.
ThirdPeerVoteStep ==
  /\ faultPhase = 5
  /\ \E m \in DOMAIN net :
       /\ m.mtype = "RVReq"
       /\ m.msrc = CombinedSuccessor
       /\ m.mdst = CombinedThird
       /\ m.mterm = 2
       /\ (UpdateTerm(CombinedThird, m) \/ HandleRVReq(CombinedThird, m))
  /\ UNCHANGED faultPhase

ThirdPeerFsync ==
  /\ faultPhase = 5
  /\ PersistCompleted(CombinedThird)
  /\ UNCHANGED faultPhase

FinishAlternativeQuorum ==
  /\ faultPhase = 5
  /\ (\/ \E m \in DOMAIN net :
          /\ m.mtype = "RVResp"
          /\ m.msrc = CombinedThird
          /\ m.mdst = CombinedSuccessor
          /\ HandleRVResp(CombinedSuccessor, m)
      \/ BecomeLeader(CombinedSuccessor))
  /\ UNCHANGED faultPhase

DualFaultNext ==
  \/ BeforeCandidateFailure
  \/ FailCandidateSelfVote
  \/ RestartCandidate
  \/ RetryCandidateElection
  \/ FsyncRetriedSelfVote
  \/ BeforeFollowerFailure
  \/ FailFollowerGrant
  \/ ThirdPeerVoteStep
  \/ ThirdPeerFsync
  \/ FinishAlternativeQuorum

DualFaultSpec == DualFaultInit /\ [][DualFaultNext]_dualVars

FaultPhaseTyped == faultPhase \in 0..5

\* These are independent safety gates against vacuous fault flags.
\* No term-two vote request can leave the candidate before its retry fsync.
NoPrematureRetryFanout ==
  faultPhase \in {1, 2, 3} =>
    /\ disk[CombinedSuccessor].term = 1
    /\ ~(\E m \in DOMAIN net :
           /\ m.mtype = "RVReq"
           /\ m.msrc = CombinedSuccessor
           /\ m.mterm = 2)

\* After the second failure the former leader is offline with a durable
\* term-only record and has not released a positive grant response.
NoUndurableFollowerGrant ==
  faultPhase = 5 =>
    /\ ~alive[CombinedPrimary]
    /\ disk[CombinedPrimary].term = 2
    /\ disk[CombinedPrimary].votedFor = Nil
    /\ ~(\E m \in DOMAIN net :
           /\ m.mtype = "RVResp"
           /\ m.msrc = CombinedPrimary
           /\ m.mdst = CombinedSuccessor
           /\ m.mterm = 2
           /\ m.granted)

FaultPhaseMonotone == [][faultPhase' >= faultPhase]_dualVars

\* EXPECTED-FAILURE reachability gate. It can only be violated after
\* both actual PersistFailed actions and a durable third-peer grant.
SomeDualFailureAlternativeQuorum ==
  /\ faultPhase = 5
  /\ ghostCommitted # {}
  /\ ghostLeaders[1] # {}
  /\ ghostLeaders[2] = {CombinedSuccessor}
  /\ role[CombinedSuccessor] = Leader
  /\ disk[CombinedSuccessor].term = 2
  /\ disk[CombinedSuccessor].votedFor = CombinedSuccessor
  /\ Len(disk[CombinedSuccessor].log) = 1
  /\ ~alive[CombinedPrimary]
  /\ disk[CombinedPrimary].term = 2
  /\ disk[CombinedPrimary].votedFor = Nil
  /\ disk[CombinedThird].term = 2
  /\ disk[CombinedThird].votedFor = CombinedSuccessor
  /\ votesGranted[CombinedSuccessor] = {CombinedSuccessor, CombinedThird}

NoDualFailureAlternativeQuorumWitness == ~SomeDualFailureAlternativeQuorum
=============================================================================