package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/utils"
)

const MaxLockTime = time.Millisecond * 10 // debug

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap          *KV
	cmdRespChans   map[IndexAndTerm]chan OpResp
	lastCmdContext map[int64]OpContext
	lastApplied    int

	// for debug
	lockStart time.Time
	lockEnd   time.Time
	lockName  string
}

func (kv *KVServer) lock(m string) {
	kv.mu.Lock()
	kv.lockStart = time.Now()
	kv.lockName = m
}

func (kv *KVServer) unlock(m string) {
	kv.lockEnd = time.Now()
	duration := kv.lockEnd.Sub(kv.lockStart)
	kv.lockName = ""
	kv.mu.Unlock()
	if duration > MaxLockTime {
		utils.Debug(utils.DServer, "S%d lock %s too long: %s", kv.me, m, duration)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(CmdArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = NewKV()
	kv.cmdRespChans = make(map[IndexAndTerm]chan OpResp)
	kv.lastCmdContext = make(map[int64]OpContext)
	kv.lastApplied = 0

	// long-time goroutines
	go kv.applier()

	return kv
}

// Handler
func (kv *KVServer) Command(args *CmdArgs, reply *CmdReply) {
	defer utils.Debug(utils.DWarn, "S%d args: %+v reply: %+v", kv.me, args, reply)

	kv.lock("isDuplicate")
	if args.Cmd.OpType != OpGet && kv.isDuplicate(args.ClientId, args.SeqId) {
		context := kv.lastCmdContext[args.ClientId]
		reply.Value, reply.Err = context.reply.Value, context.reply.Err
		kv.unlock("isDuplicate")
		return
	}
	kv.unlock("isDuplicate")

	index, term, is_leader := kv.rf.Start(*args)
	if !is_leader {
		reply.Value, reply.Err = "", ErrWrongLeader
		return
	}

	kv.lock("create chan")
	it := IndexAndTerm{index, term}
	ch := make(chan OpResp, 1)
	kv.cmdRespChans[it] = ch
	kv.unlock("create chan")

	defer func() {
		kv.lock("delete chan")
		// close(kv.cmdRespChans[index])
		delete(kv.cmdRespChans, it)
		kv.unlock("delete chan")
	}()

	t := time.NewTimer(cmd_timeout)
	defer t.Stop()

	for {
		kv.mu.Lock()
		select {
		case resp := <-ch:
			utils.Debug(utils.DServer, "S%d have applied, resp: %+v", kv.me, resp)
			reply.Value, reply.Err = resp.Value, resp.Err
			kv.mu.Unlock()
			return
		case <-t.C:
		priority:
			for {
				select {
				case resp := <-ch:
					utils.Debug(utils.DServer, "S%d have applied, resp: %+v", kv.me, resp)
					reply.Value, reply.Err = resp.Value, resp.Err
					kv.mu.Unlock()
					return
				default:
					break priority
				}
			}
			utils.Debug(utils.DServer, "S%d timeout", kv.me)
			reply.Value, reply.Err = "", ErrTimeout
			kv.mu.Unlock()
			return
		default:
			kv.mu.Unlock()
			time.Sleep(gap_time)
		}
	}
}
