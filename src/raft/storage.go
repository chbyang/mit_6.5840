package raft

import (
	"bytes"

	"6.5840/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.term) != nil || e.Encode(rf.votedTo) != nil || e.Encode(rf.log.entries) != nil ||
		e.Encode(rf.log.snapshot.Index) != nil || e.Encode(rf.log.snapshot.Term) != nil {
		panic("failed to encode some fields")
	}

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.log.snapshot.Data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.term) != nil || d.Decode(&rf.votedTo) != nil || d.Decode(&rf.log.entries) != nil ||
		d.Decode(&rf.log.snapshot.Index) != nil || d.Decode(&rf.log.snapshot.Term) != nil {
		panic("failed to decode some fields")
	}
	rf.log.compactedTo(Snapshot{Data: rf.persister.ReadSnapshot(),
		Index: rf.log.snapshot.Index, Term: rf.log.snapshot.Term})
}
