package raft

import (
	"log"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	From         int
	To           int
	Term         uint64
	LastLogIndex uint64 // Index of candidate's last log entry
	LastLogTerm  uint64 // Term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	From  int
	To    int
	Term  uint64
	Voted bool
}

type AppendEntriesArgs struct {
	From           int
	To             int
	Term           uint64  // Leader's term
	CommittedIndex uint64  // Leader's commitIndex
	PrevLogIndex   uint64  // index of log entry immediately preceding new ones
	PrevLogTerm    uint64  // term of PrevLogIndex
	Entries        []Entry // Empty for hearbeat
}

type Err int

const (
	Rejected Err = iota
	Matched
	IndexNotMatched
	TermNotMatched
)

type AppendEntriesReply struct {
	From               int
	To                 int
	Term               uint64
	Err                Err
	LastLogIndex       uint64
	ConflictTerm       uint64
	FirstConflictIndex uint64
}

type InstallSnapshotArgs struct {
	From     int
	To       int
	Term     uint64
	Snapshot Snapshot
}

type InstallSnapshotReply struct {
	From     int
	To       int
	Term     uint64
	CaughtUp bool
}

type MessageType string

const (
	Vote        MessageType = "RequestVote"
	VoteReply   MessageType = "RequestVoteReply"
	Append      MessageType = "AppendEntries"
	AppendReply MessageType = "AppendEntriesReply"
	Snap        MessageType = "InstallSnapshot"
	SnapReply   MessageType = "InstallSnapshotReply"
)

type Message struct {
	Type         MessageType
	From         int
	Term         uint64
	ArgsTerm     uint64 // term in RPC args. Used to different between term in RPC reply.
	PrevLogIndex uint64 // checking AppendEntriesReply
}

// return (termIsStale, termChanged)
func (rf *Raft) checkTerm(m Message) (bool, bool) {
	// ignore stale message.
	if m.Term < rf.term {
		return false, false
	}
	// step down if received a more up-to-date message or received a message from current leader.
	if m.Term > rf.term || (m.Type == Append || m.Type == Snap) {
		termChanged := rf.becomeFollower(m.Term)
		return true, termChanged
	}
	return true, false
}

// return true if raft peer is eligible to handle to message
func (rf *Raft) checkState(m Message) bool {
	eligible := false
	switch m.Type {
	case Vote:
		fallthrough
	case Append:
		eligible = rf.state == Follower

	case VoteReply:
		eligible = rf.state == Candidate && rf.term == m.ArgsTerm
	case AppendReply:
		//check the next index ensures it's exactly the reply corresponding to the last sent AppendEntries.
		eligible = rf.state == Leader && rf.term == m.ArgsTerm && rf.peerTrackers[m.From].nextIndex-1 == m.PrevLogIndex
	default:
		log.Fatalf("unexpected message type %v", m.Type)
	}

	// recommended to reset to not to compete with the leader. Not mandatory.
	if rf.state == Follower && (m.Type == Append || m.Type == Snap) {
		rf.resetElectionTimer()
	}

	return eligible
}

func (rf *Raft) checkMessage(m Message) (bool, bool) {
	// refresh the step down timer if received a reply.
	if m.Type == VoteReply || m.Type == AppendReply || m.Type == SnapReply {
		rf.peerTrackers[m.From].lastAck = time.Now()
	}
	ok, termChanged := rf.checkTerm(m)
	if !ok || !rf.checkState(m) {
		return false, termChanged
	}
	return true, termChanged
}
