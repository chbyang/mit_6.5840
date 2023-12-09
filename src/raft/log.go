package raft

import (
	"errors"
)

var ErrOutOfBound = errors.New("index out of bound")

type Entry struct {
	Index uint64
	Term  uint64
	Data  interface{}
}

type Snapshot struct {
	Data  []byte
	Index uint64
	Term  uint64
}

// Log manages log entries, its struct look like:
//
//	     snapshot/first.....applied....committed.....last
//	-------------|--------------------------------------|
//	  compacted           persisted log entries
type Log struct {
	// compacted log entries.
	snapshot           Snapshot
	hasPendingSnapshot bool // true if the snapshot is not yet delivered to the application.

	// persisted log entries.
	entries []Entry

	applied   uint64 // index of highest log entry known to be applied by application
	committed uint64 // index of highest log entry known to be commited by raft cluster
}

func makeLog() Log {
	log := Log{
		snapshot:           Snapshot{Data: nil, Index: 0, Term: 0},
		hasPendingSnapshot: false,
		entries:            []Entry{{Index: 0, Term: 0}}, // use a dummy entry to simplify indexing operations
		applied:            0,
		committed:          0,
	}
	return log
}

func (log *Log) toArraryIndex(index uint64) uint64 {
	return index - log.firstIndex()
}

func (log *Log) firstIndex() uint64 {
	return log.entries[0].Index
}

func (log *Log) lastIndex() uint64 {
	return log.entries[len(log.entries)-1].Index
}

func (log *Log) term(index uint64) (uint64, error) {
	if index < log.firstIndex() || index > log.lastIndex() {
		return 0, ErrOutOfBound
	}
	index = log.toArraryIndex(index)
	return log.entries[index].Term, nil
}

func (log *Log) clone(entries []Entry) []Entry {
	cloned := make([]Entry, len(entries))
	copy(cloned, entries)
	return cloned
}

func (log *Log) slice(start, end uint64) []Entry {
	if start == end {
		// can only happen when sending a heartbeat
		return nil
	}
	start = log.toArraryIndex(start)
	end = log.toArraryIndex(end)
	return log.clone(log.entries[start:end])
}

func (log *Log) truncateSuffix(index uint64) {
	if index <= log.firstIndex() || index > log.lastIndex() {
		return
	}
	index = log.toArraryIndex(index)
	if len(log.entries[index:]) > 0 {
		log.entries = log.entries[:index]
	}
}

func (log *Log) append(entries []Entry) {
	log.entries = append(log.entries, entries...)
}

func (log *Log) committedTo(index uint64) {
	if index > log.committed {
		log.committed = index
	}
}

func (log *Log) newCommittedEntries() []Entry {
	start := log.toArraryIndex(log.applied + 1)
	end := log.toArraryIndex(log.committed + 1)
	if start >= end {
		return nil
	}
	return log.clone(log.entries[start:end])
}

func (log *Log) appliedTo(index uint64) {
	if index > log.applied {
		log.applied = index
	}
}

func (log *Log) compactedTo(snapshot Snapshot) {
	suffix := make([]Entry, 0)
	suffixStart := snapshot.Index + 1
	if suffixStart <= log.lastIndex() {
		suffixStart = log.toArraryIndex(suffixStart)
		suffix = log.entries[suffixStart:]
	}
	log.entries = append(make([]Entry, 1), suffix...)
	log.snapshot = snapshot
	log.entries[0] = Entry{Index: snapshot.Index, Term: snapshot.Term}

	log.committedTo(log.snapshot.Index)
	log.appliedTo(log.snapshot.Index)
}
