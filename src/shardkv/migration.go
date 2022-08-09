package shardkv

import (
	"sync"
)

type PullDataArgs struct {
	ConfNum  int
	ShardIds []int
}

type PullDataReply struct {
	Err     Err
	ConfNum int
	Shards  map[int]*Shard
}

func (kv *ShardKV) migrationAction() {
	kv.mu.Lock()
	gid2shardIDs := kv.getShardIDsByStatus(Pulling, &kv.lastConfig)
	if len(gid2shardIDs) == 0 {
		kv.mu.Unlock()
		return
	}
	var wg sync.WaitGroup
	for gid, shardIDs := range gid2shardIDs {
		wg.Add(1)
		servers := kv.lastConfig.Groups[gid]
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			args := PullDataArgs{
				ConfNum:  configNum,
				ShardIds: shardIDs,
			}
			for _, server := range servers {
				var resp PullDataReply
				srv := kv.makeEnd(server)
				if srv.Call("ShardKV.GetShardsData", &args, &resp) && resp.Err == OK {
					kv.Execute(NewInsertShardsCommand(&resp), &OpResp{})
				}
			}
		}(servers, kv.currentConfig.Num, shardIDs)
	}
	kv.mu.Unlock()
	Debug(dServer, "G%+v {S%+v} migrationAction wait", kv.gid, kv.me)
	wg.Wait()
	Debug(dServer, "G%+v {S%+v} migrationAction done", kv.gid, kv.me)
}

func (kv *ShardKV) GetShardsData(args *PullDataArgs, reply *PullDataReply) {
	defer Debug(dServer, "G%+v {S%+v} GetShardsData: args: %+v reply: %+v", kv.gid, kv.me, args, reply)
	// only pull shards from leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()

	if kv.currentConfig.Num < args.ConfNum {
		reply.Err = ErrNotReady
		kv.mu.Unlock()
		kv.configureAction()
		return
	}

	reply.Shards = make(map[int]*Shard)
	for _, shardID := range args.ShardIds {
		reply.Shards[shardID] = kv.shards[shardID].deepCopy()
	}

	reply.ConfNum, reply.Err = args.ConfNum, OK
	kv.mu.Unlock()
}

func (kv *ShardKV) applyInsertShards(shardsInfo *PullDataReply) *OpResp {
	Debug(dServer, "G%+v {S%+v} before applyInsertShards: %+v", kv.gid, kv.me, kv.shards)
	if shardsInfo.ConfNum == kv.currentConfig.Num {
		for shardId, shardData := range shardsInfo.Shards {
			if kv.shards[shardId].Status == Pulling {
				kv.shards[shardId] = shardData.deepCopy()
				kv.shards[shardId].Status = GCing
			} else {
				Debug(dWarn, "G%+v {S%+v} shard %d is not Pulling: %+v", kv.gid, kv.me, shardId, kv.shards[shardId])
				break
			}
		}
		Debug(dServer, "G%+v {S%+v} after applyInsertShards: %+v", kv.gid, kv.me, kv.shards)
		return &OpResp{OK, ""}
	}
	return &OpResp{ErrOutDated, ""}
}
