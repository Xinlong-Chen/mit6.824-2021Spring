package shardkv

import "6.824/shardctrler"

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
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *nextConfig
		return &OpResp{OK, ""}
	}
	return &OpResp{ErrTimeoutReq, ""}
}

func (kv *ShardKV) updateShardStatus(nextConfig *shardctrler.Config) {
	kv.lastConfig = kv.currentConfig.DeepCopy()
	kv.currentConfig = nextConfig.DeepCopy()
}
