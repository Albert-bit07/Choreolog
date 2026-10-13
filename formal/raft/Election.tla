------------------------------ MODULE Election ------------------------------
EXTENDS Naturals, FiniteSets, TLC

\* A bounded model of the election/voting part of ChoreoOS election mode.
\* This is NOT a specification of log replication, commit, or snapshots.
\* Term and votedFor are durable; role and votesReceived are volatile.
\* The network may delay or drop requests and responses indefinitely.

CONSTANTS Nodes, MaxTerm

ASSUME /\ IsFiniteSet(Nodes)
       /\ Cardinality(Nodes) = 3
       /\ MaxTerm \in Nat
       /\ MaxTerm > 0

NoVote == "no-vote"
Roles == {"Follower", "Candidate", "Leader"}
Quorum == (Cardinality(Nodes) \div 2) + 1

RequestMessages ==
    {[kind |-> "Request", from |-> src, to |-> dst, term |-> t] :
        src \in Nodes, dst \in Nodes, t \in 1..MaxTerm}

ResponseMessages ==
    {[kind |-> "Response", from |-> src, to |-> dst,
      term |-> t, granted |-> granted] :
        src \in Nodes, dst \in Nodes, t \in 0..MaxTerm,
        granted \in BOOLEAN}

VoteRecords ==
    {[voter |-> voter, term |-> t, candidate |-> candidate] :
        voter \in Nodes, t \in 1..MaxTerm, candidate \in Nodes}

LeaderRecords ==
    {[leader |-> node, term |-> t] : node \in Nodes, t \in 1..MaxTerm}

VARIABLES
    currentTerm, votedFor, role, votesReceived, alive,
    messages, votesCast, leadersEver

vars == <<currentTerm, votedFor, role, votesReceived, alive,
          messages, votesCast, leadersEver>>

Init ==
    /\ currentTerm = [n \in Nodes |-> 0]
    /\ votedFor = [n \in Nodes |-> NoVote]
    /\ role = [n \in Nodes |-> "Follower"]
    /\ votesReceived = [n \in Nodes |-> {}]
    /\ alive = [n \in Nodes |-> TRUE]
    /\ messages = {}
    /\ votesCast = {}
    /\ leadersEver = {}

TypeOK ==
    /\ currentTerm \in [Nodes -> 0..MaxTerm]
    /\ votedFor \in [Nodes -> (Nodes \cup {NoVote})]
    /\ role \in [Nodes -> Roles]
    /\ votesReceived \in [Nodes -> SUBSET Nodes]
    /\ alive \in [Nodes -> BOOLEAN]
    /\ messages \subseteq (RequestMessages \cup ResponseMessages)
    /\ votesCast \subseteq VoteRecords
    /\ leadersEver \subseteq LeaderRecords

\* Persist the new term and the self-vote atomically before broadcasting.
StartElection(n) ==
    /\ alive[n]
    /\ role[n] # "Leader"
    /\ currentTerm[n] < MaxTerm
    /\ currentTerm' = [currentTerm EXCEPT ![n] = @ + 1]
    /\ votedFor' = [votedFor EXCEPT ![n] = n]
    /\ role' = [role EXCEPT ![n] = "Candidate"]
    /\ votesReceived' = [votesReceived EXCEPT ![n] = {n}]
    /\ votesCast' = votesCast \cup
         {[voter |-> n, term |-> currentTerm[n] + 1, candidate |-> n]}
    /\ messages' = messages \cup
         {[kind |-> "Request", from |-> n, to |-> peer,
           term |-> currentTerm[n] + 1] : peer \in Nodes \ {n}}
    /\ UNCHANGED <<alive, leadersEver>>

\* A voter must durably record a higher term and any granted vote before
\* the affirmative response becomes deliverable.
DeliverRequest(m) ==
    /\ m \in messages
    /\ m.kind = "Request"
    /\ alive[m.to]
    /\ LET newer == m.term > currentTerm[m.to]
           canGrant == /\ m.term >= currentTerm[m.to]
                       /\ (newer \/ votedFor[m.to] = NoVote
                                  \/ votedFor[m.to] = m.from)
           responseTerm == IF newer THEN m.term ELSE currentTerm[m.to]
       IN
         /\ currentTerm' =
              IF newer THEN [currentTerm EXCEPT ![m.to] = m.term]
              ELSE currentTerm
         /\ votedFor' =
              IF canGrant THEN [votedFor EXCEPT ![m.to] = m.from]
              ELSE IF newer THEN [votedFor EXCEPT ![m.to] = NoVote]
              ELSE votedFor
         /\ role' =
              IF newer THEN [role EXCEPT ![m.to] = "Follower"]
              ELSE role
         /\ votesReceived' =
              IF newer THEN [votesReceived EXCEPT ![m.to] = {}]
              ELSE votesReceived
         /\ votesCast' =
              IF canGrant THEN votesCast \cup
                 {[voter |-> m.to, term |-> m.term, candidate |-> m.from]}
              ELSE votesCast
         /\ messages' = (messages \ {m}) \cup
              {[kind |-> "Response", from |-> m.to, to |-> m.from,
                term |-> responseTerm, granted |-> canGrant]}
    /\ UNCHANGED <<alive, leadersEver>>

DeliverResponse(m) ==
    /\ m \in messages
    /\ m.kind = "Response"
    /\ alive[m.to]
    /\ LET newer == m.term > currentTerm[m.to]
           accept == /\ role[m.to] = "Candidate"
                     /\ m.term = currentTerm[m.to]
                     /\ m.granted
       IN
         /\ currentTerm' =
              IF newer THEN [currentTerm EXCEPT ![m.to] = m.term]
              ELSE currentTerm
         /\ votedFor' =
              IF newer THEN [votedFor EXCEPT ![m.to] = NoVote]
              ELSE votedFor
         /\ role' =
              IF newer THEN [role EXCEPT ![m.to] = "Follower"]
              ELSE role
         /\ votesReceived' =
              IF newer THEN [votesReceived EXCEPT ![m.to] = {}]
              ELSE IF accept
                   THEN [votesReceived EXCEPT ![m.to] = @ \cup {m.from}]
                   ELSE votesReceived
         /\ messages' = messages \ {m}
    /\ UNCHANGED <<alive, votesCast, leadersEver>>

\* Quorum is established from distinct voter identities, including self.
BecomeLeader(n) ==
    /\ alive[n]
    /\ role[n] = "Candidate"
    /\ Cardinality(votesReceived[n]) >= Quorum
    /\ role' = [role EXCEPT ![n] = "Leader"]
    /\ leadersEver' = leadersEver \cup
         {[leader |-> n, term |-> currentTerm[n]]}
    /\ UNCHANGED <<currentTerm, votedFor, votesReceived, alive,
                   messages, votesCast>>

\* A crash loses volatile election state but NEVER the durable term or vote.
Crash(n) ==
    /\ alive[n]
    /\ alive' = [alive EXCEPT ![n] = FALSE]
    /\ role' = [role EXCEPT ![n] = "Follower"]
    /\ votesReceived' = [votesReceived EXCEPT ![n] = {}]
    /\ UNCHANGED <<currentTerm, votedFor, messages, votesCast, leadersEver>>

Restart(n) ==
    /\ ~alive[n]
    /\ alive' = [alive EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<currentTerm, votedFor, role, votesReceived,
                   messages, votesCast, leadersEver>>

\* Arbitrary packet loss; nondelivery also models unbounded delay/partition.
Drop(m) ==
    /\ m \in messages
    /\ messages' = messages \ {m}
    /\ UNCHANGED <<currentTerm, votedFor, role, votesReceived,
                   alive, votesCast, leadersEver>>

Next ==
    \/ \E n \in Nodes : StartElection(n)
    \/ \E m \in messages : DeliverRequest(m)
    \/ \E m \in messages : DeliverResponse(m)
    \/ \E n \in Nodes : BecomeLeader(n)
    \/ \E n \in Nodes : Crash(n)
    \/ \E n \in Nodes : Restart(n)
    \/ \E m \in messages : Drop(m)

Spec == Init /\ [][Next]_vars

\* Votes are never granted to two candidates by the same voter in one term,
\* even across a crash and restart.
NoDoubleVote ==
    \A a, b \in votesCast :
        (a.voter = b.voter /\ a.term = b.term) =>
            a.candidate = b.candidate

\* Remember historical leaders so stepping down cannot hide an earlier
\* election-safety violation.
ElectionSafety ==
    \A a, b \in leadersEver :
        (a.term = b.term) => a.leader = b.leader

CandidateOwnsVote ==
    \A n \in Nodes :
        role[n] = "Candidate" => votedFor[n] = n

=============================================================================
