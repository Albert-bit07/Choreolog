------------------------- MODULE ChoreoRaftVoteReplyFaults --------------------
\* Restricted RVResp fault extension. Keep the pinned ChoreoRaft.tla intact.
\* A fault flag changes ONLY when its corresponding action executes.
EXTENDS ChoreoRaft

VARIABLE voteReplyFaults
voteReplyFaultVars == <<vars, voteReplyFaults>>

CombinedVoteReplyFaultInit ==
  /\ Init
  /\ voteReplyFaults = [dropped |-> FALSE, duplicated |-> FALSE]

CombinedVoteReplyFaultNext ==
  \/ /\ CombinedNext
     /\ UNCHANGED voteReplyFaults
  \/ /\ UNCHANGED alive
     /\ \E m \in DOMAIN net :
          /\ m.mtype = "RVResp"
          /\ m.msrc = CombinedSuccessor
          /\ m.mdst = CombinedPrimary
          /\ DropMsg(m)
          /\ voteReplyFaults' = [voteReplyFaults EXCEPT !.dropped = TRUE]
  \/ /\ UNCHANGED alive
     /\ \E m \in DOMAIN net :
          /\ m.mtype = "RVResp"
          /\ m.msrc = CombinedSuccessor
          /\ m.mdst = CombinedPrimary
          /\ DuplicateMsg(m)
          /\ voteReplyFaults' = [voteReplyFaults EXCEPT !.duplicated = TRUE]

CombinedVoteReplyFaultSpec ==
  CombinedVoteReplyFaultInit /\ [][CombinedVoteReplyFaultNext]_voteReplyFaultVars

VoteReplyFaultFlagsTyped ==
  voteReplyFaults \in [dropped : BOOLEAN, duplicated : BOOLEAN]

\* These deliberately negated reachability predicates are expected to fail
\* in dedicated witness jobs, not in the safety-model job.
NoCombinedVoteReplyDropWitness == ~voteReplyFaults.dropped
NoCombinedVoteReplyDuplicateWitness == ~voteReplyFaults.duplicated
=============================================================================
