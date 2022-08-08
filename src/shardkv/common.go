package shardkv

import "time"

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
	ErrTimeout     = "ErrTimeout"
	ErrTimeoutReq  = "ErrTimeoutReq"
	ErrNotReady    = "ErrNotReady"
	ErrOutDated    = "ErrOutDated"
)

type Err string

type OPType string

const (
	OpGet    OPType = "Get"
	OpPut    OPType = "Put"
	OpAppend OPType = "Append"
)

type CmdArgs struct {
	OpType   OPType
	Key      string
	Value    string
	ClientId int64
	SeqId    int64
}

type CmdReply struct {
	Err   Err
	Value string
}

type OpResp struct {
	Err   Err
	Value string
}

type OpContext struct {
	SeqId int64
	Reply OpResp
}

type IndexAndTerm struct {
	index int
	term  int
}

const (
	retry_timeout     time.Duration = time.Duration(1) * time.Millisecond
	cmd_timeout       time.Duration = time.Duration(2) * time.Second
	gap_time          time.Duration = time.Duration(5) * time.Millisecond
	snapshot_gap_time time.Duration = time.Duration(10) * time.Millisecond
)
