package kvraft

import (
	"sync"
	"time"
)

const maxWaitTime = 500 * time.Millisecond

type Notifier struct {
	done              sync.Cond
	maxRegisteredOpId int
}

func (kv *KVServer) getNotifier(op *Op, forced bool) *Notifier {
	if notifier, ok := kv.notifierOfClerk[op.ClerkId]; ok {
		notifier.maxRegisteredOpId = max(notifier.maxRegisteredOpId, op.OpId)
		return notifier
	}
	if !forced {
		return nil
	}
	notifier := new(Notifier)
	notifier.done = *sync.NewCond(&kv.mu)
	notifier.maxRegisteredOpId = op.OpId
	kv.notifierOfClerk[op.ClerkId] = notifier
	return notifier
}

func (kv *KVServer) notify(op *Op) {
	if notifier := kv.getNotifier(op, false); notifier != nil {
		// only the latest op can delete the notifier.
		if op.OpId == notifier.maxRegisteredOpId {
			delete(kv.notifierOfClerk, op.ClerkId)
		}
		notifier.done.Broadcast()
	}
}

func (kv *KVServer) makeAlarm(op *Op) {
	go func() {
		<-time.After(maxWaitTime)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.notify(op)
	}()
}

func (kv *KVServer) makeNotifier(op *Op) {
	kv.getNotifier(op, true)
	kv.makeAlarm(op)
}

func (kv *KVServer) wait(op *Op) {
	for !kv.killed() {
		if notifier := kv.getNotifier(op, false); notifier != nil {
			notifier.done.Wait()
		} else {
			break
		}
	}
}
