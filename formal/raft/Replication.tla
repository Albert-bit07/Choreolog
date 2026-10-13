--------------------------- MODULE Replication ---------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC

\* A deliberately bounded, atomic-step abstraction of Raft log replication.
\* Elections are abstracted to one quorum-authorized leadership transition.
\* This is not a trace refinement or proof about the production C++ runtime.
CONSTANTS Nodes, Commands, MaxTerm, MaxLog

ASSUME /\ IsFiniteSet(Nodes)
       /\ Cardinality(Nodes) = 3
       /\ IsFiniteSet(Commands)
       /\ Cardinality(Commands) > 0
       /\ MaxTerm \in Nat \ {0}
       /\ MaxLog \in Nat \ {0}

NoLeader == "none"
Quorum == (Cardinality(Nodes) \div 2) + 1
Entries == [term : 1..MaxTerm, command : Commands]

VARIABLES term, log, leader, commitIndex, committed, alive
vars == <<term, log, leader, commitIndex, committed, alive>>

Init ==
    /\ term = 0
    /\ log = [n \in Nodes |-> <<>>]
    /\ leader = NoLeader
    /\ commitIndex = [n \in Nodes |-> 0]
    /\ committed = <<>>
    /\ alive = [n \in Nodes |-> TRUE]

Prefix(s, k) == SubSeq(s, 1, k)
HasPrefix(s, p) == /\ Len(s) >= Len(p)
                   /\ Prefix(s, Len(p)) = p
LastTerm(s) == IF Len(s) = 0 THEN 0 ELSE s[Len(s)].term
UpToDate(candidate, voter) ==
    \/ LastTerm(log[candidate]) > LastTerm(log[voter])
    \/ /\ LastTerm(log[candidate]) = LastTerm(log[voter])
       /\ Len(log[candidate]) >= Len(log[voter])

TypeOK ==
    /\ term \in 0..MaxTerm
    /\ log \in [Nodes -> Seq(Entries)]
    /\ \A n \in Nodes : Len(log[n]) <= MaxLog
    /\ leader \in Nodes \cup {NoLeader}
    /\ commitIndex \in [Nodes -> 0..MaxLog]
    /\ \A n \in Nodes : commitIndex[n] <= Len(log[n])
    /\ committed \in Seq(Entries)
    /\ Len(committed) <= MaxLog
    /\ alive \in [Nodes -> BOOLEAN]

\* A single atomic election stands in for the request/vote protocol.
\* The quorum must contain the candidate and every voter must consider its
\* log at least as up-to-date as its own. Only one elected leader is modeled
\* at any point; independent, delayed old-term leaders are out of scope.
Elect(n, voters) ==
    /\ term < MaxTerm
    /\ alive[n]
    /\ voters \subseteq {v \in Nodes : alive[v]}
    /\ n \in voters
    /\ Cardinality(voters) >= Quorum
    /\ \A v \in voters : UpToDate(n, v)
    /\ term' = term + 1
    /\ leader' = n
    /\ UNCHANGED <<log, commitIndex, committed, alive>>

\* Leaders append only; each entry retains the election term and command.
AppendCommand(c) ==
    /\ leader \in Nodes
    /\ alive[leader]
    /\ Len(log[leader]) < MaxLog
    /\ log' = [log EXCEPT ![leader] =
                         Append(@, [term |-> term, command |-> c])]
    /\ UNCHANGED <<term, leader, commitIndex, committed, alive>>

\* An atomic successful AppendEntries exchange: a follower receives an
\* actual leader prefix, replacing any conflicting *uncommitted* suffix.
\* It never erases entries already committed on that follower.
Replicate(n, k) ==
    /\ leader \in Nodes
    /\ n \in Nodes \ {leader}
    /\ alive[leader] /\ alive[n]
    /\ k \in 0..Len(log[leader])
    /\ k >= commitIndex[n]
    \* An AppendEntries heartbeat or short prefix does not truncate a
    \* follower's matching suffix. Truncation requires a genuine conflict.
    /\ (k >= Len(log[n])
        \/ ~HasPrefix(log[n], Prefix(log[leader], k)))
    /\ Prefix(log[leader], k) # log[n]
    /\ log' = [log EXCEPT ![n] = Prefix(log[leader], k)]
    /\ UNCHANGED <<term, leader, commitIndex, committed, alive>>

\* Current-term-only majority commitment; this is stronger than merely
\* finding that an old-term entry appears on a majority.
Commit(k) ==
    /\ leader \in Nodes /\ alive[leader]
    /\ k \in (commitIndex[leader] + 1)..Len(log[leader])
    /\ log[leader][k].term = term
    /\ Cardinality({n \in Nodes :
              alive[n] /\ HasPrefix(log[n], Prefix(log[leader], k))})
           >= Quorum
    /\ Len(committed) <= k
    /\ HasPrefix(log[leader], committed)
    /\ committed' = Prefix(log[leader], k)
    /\ commitIndex' = [commitIndex EXCEPT ![leader] = k]
    /\ UNCHANGED <<term, log, leader, alive>>

\* Commit propagation is possible only when the follower actually has the
\* committed prefix; committed history cannot be rolled back.
AdvanceFollowerCommit(n) ==
    /\ leader \in Nodes
    /\ n \in Nodes \ {leader}
    /\ alive[n] /\ alive[leader]
    /\ commitIndex[n] < Len(committed)
    /\ HasPrefix(log[n], committed)
    /\ commitIndex' = [commitIndex EXCEPT ![n] = Len(committed)]
    /\ UNCHANGED <<term, log, leader, committed, alive>>

Crash(n) ==
    /\ alive[n]
    /\ alive' = [alive EXCEPT ![n] = FALSE]
    /\ leader' = IF leader = n THEN NoLeader ELSE leader
    /\ UNCHANGED <<term, log, commitIndex, committed>>

Restart(n) ==
    /\ ~alive[n]
    /\ alive' = [alive EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<term, log, leader, commitIndex, committed>>

Next ==
    \/ \E n \in Nodes : \E voters \in SUBSET Nodes : Elect(n, voters)
    \/ \E c \in Commands : AppendCommand(c)
    \/ \E n \in Nodes : \E k \in 0..MaxLog : Replicate(n, k)
    \/ \E k \in 1..MaxLog : Commit(k)
    \/ \E n \in Nodes : AdvanceFollowerCommit(n)
    \/ \E n \in Nodes : Crash(n)
    \/ \E n \in Nodes : Restart(n)

Spec == Init /\ [][Next]_vars

\* A shared (term,index) implies an identical entry and preceding prefix.
LogMatching ==
    \A a, b \in Nodes :
      \A i \in 1..MaxLog :
        (i <= Len(log[a]) /\ i <= Len(log[b])
         /\ log[a][i].term = log[b][i].term)
          => Prefix(log[a], i) = Prefix(log[b], i)

CommittedPrefix ==
    \A n \in Nodes :
       HasPrefix(log[n], Prefix(committed, commitIndex[n]))

CommittedHistoryOnLeader ==
    leader \in Nodes => HasPrefix(log[leader], committed)

CommittedIndexesBounded ==
    \A n \in Nodes : commitIndex[n] <= Len(committed)

=============================================================================
