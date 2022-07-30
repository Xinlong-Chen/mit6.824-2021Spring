package shardctrler

import "time"

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOpt         = "ErrOpt"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type OpType string

const (
	OpJoin  OpType = "join"
	OpLeave OpType = "leave"
	OpMove  OpType = "move"
	OpQuery OpType = "query"
)

type CommandArgs struct {
	Op       OpType
	ClientId int64
	SeqId    int64
	Servers  map[int][]string // for Join
	GIDs     []int            // for Leave
	Shard    int              // for Move
	GID      int              // for Move
	Num      int              // for Query
}

type CommandReply struct {
	Err    Err
	Config Config
}

type Op CommandArgs

type OpResp struct {
	Err    Err
	Config Config
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
