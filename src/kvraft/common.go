package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrTimeoutReq  = "ErrTimeoutReq"
)

type Err string

// Put or Append
type CmdArgs struct {
	Cmd      Op
	ClientId int64
	SeqId    int64
}

type CmdReply struct {
	Err   Err
	Value string
}

type OPType string

const (
	OpGet    OPType = "Get"
	OpPut    OPType = "Put"
	OpAppend OPType = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType OPType
	Key    string
	Value  string
}

type OpResp struct {
	Err   Err
	Value string
}

type OpContext struct {
	seqId int64
	reply OpResp
}

type IndexAndTerm struct {
	index int
	term  int
}

const (
	retry_timeout time.Duration = time.Duration(1) * time.Millisecond
	cmd_timeout   time.Duration = time.Duration(2) * time.Second
	gap_time      time.Duration = time.Duration(5) * time.Millisecond
)
