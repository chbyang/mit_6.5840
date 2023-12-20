package kvraft

import "log"

func (kv *KVServer) isApplied(op *Op) bool {
	maxAppliedOpId, ok := kv.maxAppliedOpIdOfClerk[op.ClerkId]
	return ok && maxAppliedOpId >= op.OpId
}

func (kv *KVServer) applyClientOp(op *Op) {
	switch op.OpType {
	case "Get":
		// only write ops are applied to the db.
	case "Put":
		kv.db[op.Key] = op.Value
	case "Append":
		kv.db[op.Key] += op.Value
	default:
		log.Fatalf("unexpected client op type %v", op.OpType)
	}
}

func (kv *KVServer) maybeApplyClientOp(op *Op) {
	if !kv.isApplied(op) {
		kv.applyClientOp(op)
		kv.maxAppliedOpIdOfClerk[op.ClerkId] = op.OpId
		kv.notify(op)
	}
}

func (kv *KVServer) executor() {
	for m := range kv.applyCh {
		if kv.killed() {
			break
		}
		kv.mu.Lock()
		if m.SnapshotValid {
			kv.ingestSnapshot(m.Snapshot)
		} else {
			op := m.Command.(*Op)
			if kv.isNoOp(op) {

			} else {
				kv.maybeApplyClientOp(op)
			}
			if kv.gcEnabled && kv.approachGCLimit() {
				kv.checkpoint(m.CommandIndex)
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) waitUntilAppliedOrTimeout(op *Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.isApplied(op) {
		// send command to raft
		if !kv.propose(op) {
			return ErrWrongLeader, ""
		}
		kv.makeNotifier(op)
		kv.wait(op)

	}
	if kv.isApplied(op) {
		value := ""
		if op.OpType == "Get" {
			value = kv.db[op.Key]
		}
		return OK, value
	}
	return ErrNotApplied, ""
}
