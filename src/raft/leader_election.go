package raft

import (
	"math/rand"
	"time"
)

// base timeout T; election timeout in range [T,2T)
const baseElectionTimeout = 300

func (rf *Raft) pastElectionTimeout() bool {
	return time.Since(rf.lastElection) > rf.electionTimeout
}

func (rf *Raft) resetElectionTimer() {
	electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
}

func (rf *Raft) becomeFollower(term uint64) bool {
	rf.state = Follower
	if term > rf.term {
		rf.term = term
		rf.votedTo = None
		return true
	}
	return false
}

func (rf *Raft) becomeCandidate() {
	defer rf.persist()
	rf.state = Candidate
	rf.term++
	rf.votedMe = make([]bool, len(rf.peers))
	rf.votedTo = rf.me
	rf.resetElectionTimer()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.resetTrackedIndexes()
}

func (rf *Raft) makeRequestVoteArgs(to int) *RequestVoteArgs {
	lastLogIndex := rf.log.lastIndex()
	lastLogTerm, _ := rf.log.term(lastLogIndex)
	args := &RequestVoteArgs{From: rf.me, To: to, Term: rf.term,
		LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	return args
}

func (rf *Raft) eligibleToGrantVote(candidateLastLogIndex, candidateLastLogTerm uint64) bool {
	lastLogIndex := rf.log.lastIndex()
	lastLogTerm, _ := rf.log.term(lastLogIndex)
	return candidateLastLogTerm > lastLogTerm ||
		(candidateLastLogTerm == lastLogTerm && candidateLastLogIndex >= lastLogIndex)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.From = rf.me
	reply.To = args.From
	reply.Term = rf.term
	reply.Voted = false

	m := Message{Type: Vote, From: args.From, Term: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		reply.Term = rf.term
		defer rf.persist()
	}
	if !ok {
		return
	}
	// Chaobin: votedTo 可以分票。什么时候是args.From?
	if (rf.votedTo == None || rf.votedTo == args.From) &&
		rf.eligibleToGrantVote(args.LastLogIndex, args.LastLogTerm) {
		rf.votedTo = args.From
		rf.resetElectionTimer()
		reply.Voted = true
	}
}

func (rf *Raft) quorumVoted() bool {
	votes := 1
	for i, votedMe := range rf.votedMe {
		if i != rf.me && votedMe {
			votes++
		}
	}
	return 2*votes > len(rf.peers)
}

func (rf *Raft) handleRequestVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	m := Message{Type: VoteReply, From: reply.From, Term: reply.Term, ArgsTerm: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		defer rf.persist()
	}
	if !ok {
		return
	}
	if reply.Voted {
		rf.votedMe[reply.From] = true
		if rf.quorumVoted() {
			rf.becomeLeader()
		}
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	if ok := rf.peers[args.To].Call("Raft.RequestVote", args, &reply); ok {
		rf.handleRequestVoteReply(args, &reply)
	}
}

func (rf *Raft) broadcastRequestVote() {
	for i := range rf.peers {
		if i != rf.me {
			args := rf.makeRequestVoteArgs(i)
			go rf.sendRequestVote(args)
		}
	}
}
