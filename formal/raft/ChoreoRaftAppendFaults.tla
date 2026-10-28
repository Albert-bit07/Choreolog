-------------------------- MODULE ChoreoRaftAppendFaults -----------------------
\* Bounded AppendEntries request fault extension of the pinned model.
\* Preserve ChoreoRaft.tla byte-for-byte for existing witness provenance.
EXTENDS ChoreoRaft

\* Ghost flags record the NEW fault action, so witness gates cannot be
\* satisfied merely because an AppendEntries request was never sent.
VARIABLE appendFaults
appendFaultVars == <<vars, appendFaults>>

CombinedAppendFaultInit ==
  /\ Init
  /\ appendFaults = [dropped |-> FALSE, duplicated |-> FALSE]

CombinedAppendFaultNext ==
  \/ /\ CombinedNext
     /\ UNCHANGED appendFaults
  \/ /\ UNCHANGED alive
     /\ \E m \in DOMAIN net :
          /\ m.mtype = "AEReq"
          /\ m.msrc = CombinedPrimary
          /\ m.mdst = CombinedSuccessor
          /\ DropMsg(m)
          /\ appendFaults' = [appendFaults EXCEPT !.dropped = TRUE]
  \/ /\ UNCHANGED alive
     /\ \E m \in DOMAIN net :
          /\ m.mtype = "AEReq"
          /\ m.msrc = CombinedPrimary
          /\ m.mdst = CombinedSuccessor
          /\ DuplicateMsg(m)
          /\ appendFaults' = [appendFaults EXCEPT !.duplicated = TRUE]

CombinedAppendFaultSpec ==
  CombinedAppendFaultInit /\ [][CombinedAppendFaultNext]_appendFaultVars

AppendFaultFlagsTyped ==
  appendFaults \in [dropped : BOOLEAN, duplicated : BOOLEAN]

\* These NEGATED witness predicates are expected to fail in their dedicated
\* reachability jobs. They are not safety invariants.
NoCombinedAppendDropWitness == ~appendFaults.dropped
NoCombinedAppendDuplicateWitness == ~appendFaults.duplicated
=============================================================================
