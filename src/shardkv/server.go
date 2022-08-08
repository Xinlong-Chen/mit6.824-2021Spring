package shardkv

import (
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	makeEnd func(string) *labrpc.ClientEnd
	gid     int
	// ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	shards       map[int]*Shard
	cmdRespChans map[IndexAndTerm]chan OpResp
	lastApplied  int
	lastSnapshot int

	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config
	sc            *shardctrler.Clerk
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//fmt.Printf("---kill\n")
	kv.doSnapshot(kv.lastApplied)
	kv.rf.Kill()
	Debug(dWarn, "G%+v {S%+v} close shards: %+v config: %+v", kv.gid, kv.me, kv.shards, kv.currentConfig)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(CmdArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(PullDataReply{})
	labgob.Register(PullDataArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.makeEnd = makeEnd
	kv.gid = gid
	// kv.ctrlers = ctrlers

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg, 5)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sc = shardctrler.MakeClerk(ctrlers)

	// Your initialization code here.
	kv.shards = make(map[int]*Shard)
	kv.cmdRespChans = make(map[IndexAndTerm]chan OpResp)
	kv.lastApplied = 0
	kv.lastSnapshot = 0

	// load data from persister
	kv.setSnapshot(persister.ReadSnapshot())

	// long-time goroutines
	go kv.applier()
	go kv.snapshoter()
	kv.startMonitor()

	return kv
}
