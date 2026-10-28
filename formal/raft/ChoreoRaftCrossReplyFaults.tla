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
                         appendDuplicated |-> FALSE,
                         orderedAppendDropped |-> FALSE,
                         orderedAppendDuplicated |-> FALSE]

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
          /\ crossReplyFaults' = [crossReplyFaults EXCEPT
                                    !.appendDropped = TRUE,
                                    !.orderedAppendDropped =
                                      crossReplyFaults.orderedAppendDropped \/
                                      crossReplyFaults.voteDuplicated]
  \/ /\ UNCHANGED alive
     /\ \E m \in DOMAIN net :
          /\ m.mtype = "AEResp"
          /\ m.msrc = CombinedSuccessor
          /\ m.mdst = CombinedPrimary
          /\ DuplicateMsg(m)
          /\ crossReplyFaults' = [crossReplyFaults EXCEPT
                                    !.appendDuplicated = TRUE,
                                    !.orderedAppendDuplicated =
                                      crossReplyFaults.orderedAppendDuplicated \/
                                      crossReplyFaults.voteDuplicated]

CrossReplySpec == CrossReplyInit /\ [][CrossReplyNext]_crossReplyVars

CrossReplyFlagsTyped ==
  crossReplyFaults \in [voteDuplicated : BOOLEAN,
                         appendDropped : BOOLEAN,
                         appendDuplicated : BOOLEAN,
                         orderedAppendDropped : BOOLEAN,
                         orderedAppendDuplicated : BOOLEAN]

\* An ordered flag is set only by an AppendEntries fault action that occurs
\* after the RequestVote response duplication was already observed.
CrossReplyOrderingSound ==
  /\ (crossReplyFaults.orderedAppendDropped =>
        (crossReplyFaults.voteDuplicated /\ crossReplyFaults.appendDropped))
  /\ (crossReplyFaults.orderedAppendDuplicated =>
        (crossReplyFaults.voteDuplicated /\ crossReplyFaults.appendDuplicated))

\* Require actual action order within one behavior, not merely co-occurrence.
NoCrossReplyVoteDupAppendDropWitness ==
  ~crossReplyFaults.orderedAppendDropped
NoCrossReplyVoteDupAppendDupWitness ==
  ~crossReplyFaults.orderedAppendDuplicated
=============================================================================
