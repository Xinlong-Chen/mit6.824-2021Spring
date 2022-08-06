package shardkv

import "6.824/raft"

// Handler
func (kv *ShardKV) Command(args *CmdArgs, reply *CmdReply) {
	kv.mu.Lock()
	if args.OpType != OpGet && kv.isDuplicate(args.ClientId, args.SeqId) {
		context := kv.LastCmdContext[args.ClientId]
		reply.Value, reply.Err = context.Reply.Value, context.Reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	kv.Execute(NewOperationCommand(args), reply)
}

func (kv *ShardKV) canServe(shardID int) bool {
	if _, ok := kv.shards[shardID]; !ok {
		kv.shards[shardID] = NewShard()
	}
	return kv.currentConfig.Shards[shardID] == kv.gid && (kv.shards[shardID].Status == Serving || kv.shards[shardID].Status == GCing)
}

func (kv *ShardKV) applyOperation(msg *raft.ApplyMsg, cmd *CmdArgs) *OpResp {
	shardID := key2shard(cmd.Key)
	if kv.canServe(shardID) {
		if cmd.OpType != OpGet && kv.isDuplicate(cmd.ClientId, cmd.SeqId) {
			context := kv.LastCmdContext[cmd.ClientId]
			return &context.Reply

		} else {
			var resp OpResp
			resp.Value, resp.Err = kv.Opt(cmd, shardID)
			kv.LastCmdContext[cmd.ClientId] = OpContext{
				SeqId: cmd.SeqId,
				Reply: resp,
			}
			return &resp
		}
	}
	return &OpResp{ErrWrongGroup, ""}
}
