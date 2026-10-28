-------------------------- MODULE ChoreoRaftVoteFaults -------------------------
\* Restricted network-fault extension of the pinned ChoreoRaft model.
\* Keep ChoreoRaft.tla byte-for-byte unchanged: existing model-generated
\* witness provenance is bound to that source SHA-256 and must stay strict.
EXTENDS ChoreoRaft

\* Add only RVReq/RVResp drops and duplicates from the two designated
\* candidates to the existing asymmetric combined action subset. This is
\* not arbitrary AppendEntries packet loss, node failure, or full refinement.
CombinedVoteFaultNext ==
  \/ CombinedNext
  \/ /\ UNCHANGED alive
     /\ \E m \in DOMAIN net :
          /\ m.mtype \in {"RVReq", "RVResp"}
          /\ m.msrc \in {CombinedPrimary, CombinedSuccessor}
          /\ DuplicateMsg(m)
  \/ /\ UNCHANGED alive
     /\ \E m \in DOMAIN net :
          /\ m.mtype \in {"RVReq", "RVResp"}
          /\ m.msrc \in {CombinedPrimary, CombinedSuccessor}
          /\ DropMsg(m)
CombinedVoteFaultSpec == Init /\ [][CombinedVoteFaultNext]_vars

\* Negated predicates are EXPECTED-FAILURE reachability gates, not safety
\* invariants. In this slice the third peer cannot process the primary's
\* RV request; after it is durably sent, its absence requires a drop.
SomeCombinedDuplicatedVote ==
  \E m \in DOMAIN net :
    /\ m.mtype \in {"RVReq", "RVResp"}
    /\ net[m] > 1
NoCombinedDuplicatedVoteWitness == ~SomeCombinedDuplicatedVote

SomeCombinedDroppedThirdVote ==
  /\ disk[CombinedPrimary].term = 1
  /\ mem[CombinedPrimary].term = 1
  /\ role[CombinedPrimary] \in {Candidate, Leader}
  /\ ~(\E m \in DOMAIN net :
           /\ m.mtype = "RVReq"
           /\ m.msrc = CombinedPrimary
           /\ m.mdst = CombinedThird)
NoCombinedDroppedThirdVoteWitness == ~SomeCombinedDroppedThirdVote

\* This witness is intentionally different from the third-peer drop already
\* permitted by CombinedNext. The successor has neither staged nor persisted
\* the higher term, so it cannot have processed the request through UpdateTerm
\* or HandleRVReq; the request must have been removed by the NEW fault action.
SomeCombinedDroppedSuccessorVote ==
  /\ disk[CombinedPrimary].term = 1
  /\ mem[CombinedPrimary].term = 1
  /\ role[CombinedPrimary] = Candidate
  /\ disk[CombinedSuccessor].term = 0
  /\ mem[CombinedSuccessor].term = 0
  /\ pending[CombinedSuccessor] = Nil
  /\ role[CombinedSuccessor] = Follower
  /\ ~(\E m \in DOMAIN net :
           /\ m.mtype = "RVReq"
           /\ m.msrc = CombinedPrimary
           /\ m.mdst = CombinedSuccessor)
NoCombinedDroppedSuccessorVoteWitness == ~SomeCombinedDroppedSuccessorVote
=============================================================================
