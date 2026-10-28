------------------------- MODULE ChoreoRaftAppendReplyFaults --------------------
\* Restricted AEResp fault extension. Keep the pinned ChoreoRaft.tla intact.
\* A fault flag changes ONLY when its corresponding action executes.
EXTENDS ChoreoRaft

VARIABLE appendReplyFaults
appendReplyFaultVars == <<vars, appendReplyFaults>>

CombinedAppendReplyFaultInit ==
  /\ Init
  /\ appendReplyFaults = [dropped |-> FALSE, duplicated |-> FALSE]

CombinedAppendReplyFaultNext ==
  \/ /\ CombinedNext
     /\ UNCHANGED appendReplyFaults
  \/ /\ UNCHANGED alive
     /\ \E m \in DOMAIN net :
          /\ m.mtype = "AEResp"
          /\ m.msrc = CombinedSuccessor
          /\ m.mdst = CombinedPrimary
          /\ DropMsg(m)
          /\ appendReplyFaults' = [appendReplyFaults EXCEPT !.dropped = TRUE]
  \/ /\ UNCHANGED alive
     /\ \E m \in DOMAIN net :
          /\ m.mtype = "AEResp"
          /\ m.msrc = CombinedSuccessor
          /\ m.mdst = CombinedPrimary
          /\ DuplicateMsg(m)
          /\ appendReplyFaults' = [appendReplyFaults EXCEPT !.duplicated = TRUE]

CombinedAppendReplyFaultSpec ==
  CombinedAppendReplyFaultInit /\ [][CombinedAppendReplyFaultNext]_appendReplyFaultVars

AppendReplyFaultFlagsTyped ==
  appendReplyFaults \in [dropped : BOOLEAN, duplicated : BOOLEAN]

\* These deliberately negated reachability predicates are expected to fail
\* in dedicated witness jobs, not in the safety-model job.
NoCombinedAppendReplyDropWitness == ~appendReplyFaults.dropped
NoCombinedAppendReplyDuplicateWitness == ~appendReplyFaults.duplicated
=============================================================================
