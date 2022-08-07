package shardkv

import (
	"6.824/shardctrler"
)

func (kv *ShardKV) configureAction() {
	canPerformNextConfig := true
	kv.mu.Lock()
	for _, shard := range kv.shards {
		if shard.Status != Serving {
			canPerformNextConfig = false
			break
		}
	}
	currentConfigNum := kv.currentConfig.Num
	kv.mu.Unlock()
	if canPerformNextConfig {
		nextConfig := kv.sc.Query(currentConfigNum + 1)
		if nextConfig.Num == currentConfigNum+1 {
			kv.Execute(NewConfigurationCommand(&nextConfig), &CmdReply{})
		}
	}
}

func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *OpResp {
	if nextConfig.Num == kv.currentConfig.Num+1 {
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = kv.currentConfig.DeepCopy()
		kv.currentConfig = nextConfig.DeepCopy()
		return &OpResp{OK, ""}
	}
	return &OpResp{ErrTimeoutReq, ""}
}

func (kv *ShardKV) updateShardStatus(nextConfig *shardctrler.Config) {
	// special judge
	if nextConfig.Num == 1 {
		shards := kv.getAllShards(nextConfig)
		for _, shard := range shards {
			kv.shards[shard] = NewShard(Serving)
		}
		return
	}

	newShards := kv.getAllShards(nextConfig)
	nowShards := kv.getAllShards(&kv.currentConfig)
	// loss shard
	for _, nowShard := range nowShards {
		if nextConfig.Shards[nowShard] != kv.gid {
			// BePulling
			kv.shards[nowShard].Status = Serving
		}
	}
	// get shard
	for _, newShard := range newShards {
		if kv.currentConfig.Shards[newShard] != kv.gid {
			// Pulling
			kv.shards[newShard] = NewShard(Serving)
		}
	}
}

func (kv *ShardKV) getAllShards(nextConfig *shardctrler.Config) []int {
	var shards []int
	for shard, gid := range nextConfig.Shards {
		if gid == kv.gid {
			shards = append(shards, shard)
		}
	}
	return shards
}
