package raft

import (
	"time"
)

// if the peer is not active in this duration, it is considered inactive
const activeWindowWidth = 2 * baseElectionTimeout * time.Millisecond

type PeerTracker struct {
	nextIndex  uint64 // for each server, index of the next log entry to send to that server
	matchIndex uint64 //for each server, index of highest log entry known to be replicated on server

	lastAck time.Time
}

func (rf *Raft) resetTrackedIndexes() {
	for i := range rf.peerTrackers {
		rf.peerTrackers[i].nextIndex = rf.log.lastIndex() + 1
		rf.peerTrackers[i].matchIndex = 0
	}
}

func (rf *Raft) quorumActive() bool {
	activePeers := 1
	for i, tracker := range rf.peerTrackers {
		if i != rf.me && time.Since(tracker.lastAck) <= activeWindowWidth {
			activePeers++
		}
	}
	return 2*activePeers > len(rf.peers)
}
