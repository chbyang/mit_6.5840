package kvraft

import (
	"bytes"

	"6.5840/labgob"
)

// snapshot starts if raft state is higher than GCRatio * maxRaftStateSize.
const GCRatio = 0.8

func (kv *KVServer) ingestSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.db) != nil || d.Decode(&kv.maxAppliedOpIdOfClerk) != nil {
		panic("failed to decode some fields")
	}
}

func (kv *KVServer) approachGCLimit() bool {
	return float32(kv.persister.RaftStateSize()) > GCRatio*float32(kv.maxraftstate)
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.db) != nil || e.Encode(kv.maxAppliedOpIdOfClerk) != nil {
		panic("failed to encode some fields")
	}
	return w.Bytes()
}

func (kv *KVServer) checkpoint(index int) {
	snapshot := kv.makeSnapshot()
	kv.rf.Snapshot(index, snapshot)
}
