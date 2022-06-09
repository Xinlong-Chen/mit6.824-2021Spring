package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Errno int

const (
	SuccessCode Errno = iota
	ServiceErrCode
	ParaErrCode
)

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
	WAIT
	STOP
)

// Add your RPC definitions here.
type GetTaskArgs struct {
}

type GetTaskReply struct {
	Type      TaskType
	Filenames []string
	Task_no   int
	NReduce   int
	Err       Errno
}

type FinishTaskArgs struct {
	Type    TaskType
	Task_no int
}

type FinishTaskReply struct {
	Err Errno
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
