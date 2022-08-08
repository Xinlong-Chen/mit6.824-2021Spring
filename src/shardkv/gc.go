package shardkv

import "sync"

func (kv *ShardKV) gcAction() {
	kv.mu.Lock()
	gid2shardIDs := kv.getShardIDsByStatus(GCing, &kv.lastConfig)
	var wg sync.WaitGroup
	for gid, shardIDs := range gid2shardIDs {
		wg.Add(1)
		servers := kv.lastConfig.Groups[gid]
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			args := PullDataArgs{configNum, shardIDs}
			for _, server := range servers {
				var reply PullDataReply
				srv := kv.makeEnd(server)
				if srv.Call("ShardKV.DeleteShardsData", &args, &reply) && reply.Err == OK {
					kv.Execute(NewDeleteShardsCommand(&args), &OpResp{})
				}
			}
		}(servers, kv.currentConfig.Num, shardIDs)
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) DeleteShardsData(args *PullDataArgs, reply *PullDataReply) {
	// only delete shards when role is leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.currentConfig.Num > args.ConfNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var resp OpResp
	kv.Execute(NewDeleteShardsCommand(args), &resp)

	reply.Err = resp.Err
}

func (kv *ShardKV) applyDeleteShards(shardsInfo *PullDataArgs) *OpResp {
	if shardsInfo.ConfNum == kv.currentConfig.Num {
		for _, shardId := range shardsInfo.ShardIds {
			shard := kv.shards[shardId]
			if shard.Status == GCing {
				shard.Status = Serving
			} else if shard.Status == BePulling {
				kv.shards[shardId] = NewShard(Serving)
			} else {
				break
			}
		}
		return &OpResp{OK, ""}
	}
	return &OpResp{OK, ""}
}
