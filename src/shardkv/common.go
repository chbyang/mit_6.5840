package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	Put    = "Put"
	Append = "Append"
	Get    = "Get"
)

type Err string

type CommandArgs struct {
	Key      string
	Value    string
	Op       string
	Seq      int
	ClientId int64
}

type CommandReply struct {
	Err   Err
	Value string
}
