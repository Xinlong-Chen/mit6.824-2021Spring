package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	idle TaskStatus = iota
	in_progress
	completed
)

type Task struct {
	tno       int
	filenames []string
	status    TaskStatus
	startTime time.Time
}

type CoordinatorStatus int

const (
	MAP_PHASE CoordinatorStatus = iota
	REDUCE_PHASE
	FINISH_PHASE
)

type Coordinator struct {
	// Your definitions here.
	tasks   []Task
	nReduce int
	nMap    int
	status  CoordinatorStatus
	mu      sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	finish_flag := c.IsAllFinish()
	if finish_flag {
		c.NextPhase()
	}
	for i := 0; i < len(c.tasks); i++ {
		if c.tasks[i].status == idle {
			log.Printf("send task %d to worker\n", i)
			reply.Err = SuccessCode
			reply.Task_no = i
			reply.Filenames = c.tasks[i].filenames
			if c.status == MAP_PHASE {
				reply.Type = MAP
				reply.NReduce = c.nReduce
			} else if c.status == REDUCE_PHASE {
				reply.NReduce = 0
				reply.Type = REDUCE
			} else {
				log.Fatal("unexpected status")
			}
			c.tasks[i].startTime = time.Now()
			c.tasks[i].status = in_progress
			return nil
		} else if c.tasks[i].status == in_progress {
			curr := time.Now()
			if curr.Sub(c.tasks[i].startTime) > time.Second*10 {
				log.Printf("resend task %d to worker\n", i)
				reply.Err = SuccessCode
				reply.Task_no = i
				reply.Filenames = c.tasks[i].filenames
				if c.status == MAP_PHASE {
					reply.Type = MAP
					reply.NReduce = c.nReduce
				} else if c.status == REDUCE_PHASE {
					reply.NReduce = 0
					reply.Type = REDUCE
				} else {
					log.Fatal("unexpected status")
				}
				c.tasks[i].startTime = time.Now()
				return nil
			}
		}
	}
	reply.Err = SuccessCode
	reply.Type = WAIT
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Task_no >= len(c.tasks) || args.Task_no < 0 {
		reply.Err = ParaErrCode
		return nil
	}
	c.tasks[args.Task_no].status = completed
	if c.IsAllFinish() {
		c.NextPhase()
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// coordinator init code
func (c *Coordinator) Init(files []string, nReduce int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Println("init coordinator")

	// make map tasks
	log.Println("make map tasks")
	tasks := make([]Task, len(files))
	for i, file := range files {
		tasks[i].tno = i
		tasks[i].filenames = []string{file}
		tasks[i].status = idle
	}

	// init coordinator
	c.tasks = tasks
	c.nReduce = nReduce
	c.nMap = len(files)
	c.status = MAP_PHASE
}

func (c *Coordinator) MakeReduceTasks() {
	// make reduce tasks
	log.Println("make reduce tasks")
	tasks := make([]Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		tasks[i].tno = i
		files := make([]string, c.nMap)
		for j := 0; j < c.nMap; j++ {
			filename := fmt.Sprintf("mr-%d-%d", j, i)
			files[j] = filename
		}
		tasks[i].filenames = files
		tasks[i].status = idle
	}
	c.tasks = tasks
}

func (c *Coordinator) IsAllFinish() bool {
	for i := len(c.tasks) - 1; i >= 0; i-- {
		if c.tasks[i].status != completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) NextPhase() {
	if c.status == MAP_PHASE {
		log.Println("change to REDUCE_PHASE")
		c.MakeReduceTasks()
		c.status = REDUCE_PHASE
	} else if c.status == REDUCE_PHASE {
		log.Println("change to FINISH_PHASE")
		c.status = FINISH_PHASE
	} else {
		log.Println("unexpected status change!")
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status == FINISH_PHASE {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Init(files, nReduce)

	c.server()
	return &c
}
