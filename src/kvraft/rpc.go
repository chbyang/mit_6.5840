package kvraft

type Err string

const (
	OK             = "OK"
	ErrNotApplied  = "ErrNotApplied"
	ErrWrongLeader = "ErrWrongLeader"
)

// Put or Append
type PutAppendArgs struct {
	Key    string
	Value  string
	OpType string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	OpId    int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId int64
	OpId    int
}

type GetReply struct {
	Err   Err
	Value string
}
