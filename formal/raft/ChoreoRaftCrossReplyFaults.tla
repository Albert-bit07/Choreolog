------------------------- MODULE ChoreoRaftCrossReplyFaults -------------------
\* Restricted cross-protocol reply faults. Pinned ChoreoRaft.tla is unchanged.
\* Separate action-only flags prevent vacuous mixed-fault witnesses.
EXTENDS ChoreoRaft

VARIABLE crossReplyFaults
crossReplyVars == <<vars, crossReplyFaults>>

CrossReplyInit ==
  /\ Init
  /\ crossReplyFaults = [voteDuplicated |-> FALSE,
                         appendDropped |-> FALSE,
                         appendDuplicated |-> FALSE]

CrossReplyNext ==
  \/ /\ CombinedNext
     /\ UNCHANGED crossReplyFaults
  \/ /\ UNCHANGED alive
     /\ \E m \in DOMAIN net :
          /\ m.mtype = "RVResp"
          /\ m.msrc = CombinedSuccessor
          /\ m.mdst = CombinedPrimary
          /\ DuplicateMsg(m)
          /\ crossReplyFaults' = [crossReplyFaults EXCEPT !.voteDuplicated = TRUE]
  \/ /\ UNCHANGED alive
     /\ \E m \in DOMAIN net :
          /\ m.mtype = "AEResp"
          /\ m.msrc = CombinedSuccessor
          /\ m.mdst = CombinedPrimary
          /\ DropMsg(m)
          /\ crossReplyFaults' = [crossReplyFaults EXCEPT !.appendDropped = TRUE]
  \/ /\ UNCHANGED alive
     /\ \E m \in DOMAIN net :
          /\ m.mtype = "AEResp"
          /\ m.msrc = CombinedSuccessor
          /\ m.mdst = CombinedPrimary
          /\ DuplicateMsg(m)
          /\ crossReplyFaults' = [crossReplyFaults EXCEPT !.appendDuplicated = TRUE]

CrossReplySpec == CrossReplyInit /\ [][CrossReplyNext]_crossReplyVars

CrossReplyFlagsTyped ==
  crossReplyFaults \in [voteDuplicated : BOOLEAN,
                         appendDropped : BOOLEAN,
                         appendDuplicated : BOOLEAN]

\* Both fault actions must execute in ONE behavior, not separate traces.
NoCrossReplyVoteDupAppendDropWitness ==
  ~(crossReplyFaults.voteDuplicated /\ crossReplyFaults.appendDropped)
NoCrossReplyVoteDupAppendDupWitness ==
  ~(crossReplyFaults.voteDuplicated /\ crossReplyFaults.appendDuplicated)
=============================================================================
