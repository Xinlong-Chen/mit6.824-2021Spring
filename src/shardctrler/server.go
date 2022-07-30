package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.
	configs        *ConfigModel
	cmdRespChans   map[IndexAndTerm]chan OpResp
	LastCmdContext map[int64]OpContext
	lastApplied    int
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configs = NewConfigModel(me)
	sc.cmdRespChans = make(map[IndexAndTerm]chan OpResp)
	sc.LastCmdContext = make(map[int64]OpContext)
	sc.lastApplied = 0

	// long-time goroutines
	go sc.applier()

	return sc
}

// Handler
func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	defer Debug(dWarn, "S%d args: %+v reply: %+v", sc.me, args, reply)

	if !sc.configs.isLegal(args.Op) {
		reply.Config, reply.Err = Config{}, ErrOpt
	}

	sc.mu.Lock()
	if args.Op != OpQuery && sc.isDuplicate(args.ClientId, args.SeqId) {
		context := sc.LastCmdContext[args.ClientId]
		reply.Config, reply.Err = context.Reply.Config, context.Reply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	index, term, is_leader := sc.rf.Start(Op(*args))
	if !is_leader {
		reply.Config, reply.Err = Config{}, ErrWrongLeader
		return
	}

	sc.mu.Lock()
	it := IndexAndTerm{index, term}
	ch := make(chan OpResp, 1)
	sc.cmdRespChans[it] = ch
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		// close(sc.cmdRespChans[index])
		delete(sc.cmdRespChans, it)
		sc.mu.Unlock()
		close(ch)
	}()

	t := time.NewTimer(cmd_timeout)
	defer t.Stop()

	for {
		sc.mu.Lock()
		select {
		case resp := <-ch:
			Debug(dServer, "S%d have applied, resp: %+v", sc.me, resp)
			reply.Config, reply.Err = resp.Config, resp.Err
			sc.mu.Unlock()
			return
		case <-t.C:
		priority:
			for {
				select {
				case resp := <-ch:
					Debug(dServer, "S%d have applied, resp: %+v", sc.me, resp)
					reply.Config, reply.Err = resp.Config, resp.Err
					sc.mu.Unlock()
					return
				default:
					break priority
				}
			}
			Debug(dServer, "S%d timeout", sc.me)
			reply.Config, reply.Err = Config{}, ErrTimeout
			sc.mu.Unlock()
			return
		default:
			sc.mu.Unlock()
			time.Sleep(gap_time)
		}
	}
}
