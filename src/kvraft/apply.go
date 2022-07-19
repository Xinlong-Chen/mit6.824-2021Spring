package kvraft

import "6.824/utils"

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			utils.Debug(utils.DServer, "S%d apply msg: %+v", kv.me, msg)
			if msg.SnapshotValid {

			} else if msg.CommandValid {

				kv.lock("apply msg")

				if msg.CommandIndex <= kv.lastApplied {
					utils.Debug(utils.DWarn, "S%d out time apply(%d <= %d): %+v", kv.me, msg.CommandIndex, kv.lastApplied, msg)

					kv.unlock("apply msg")
					continue
				}
				kv.lastApplied = msg.CommandIndex

				var resp OpResp
				args := msg.Command.(CmdArgs)

				if args.Cmd.OpType != OpGet && kv.isDuplicate(args.ClientId, args.SeqId) {
					context := kv.lastCmdContext[args.ClientId]
					resp = context.reply
				} else {
					resp.Value, resp.Err = kv.Opt(args.Cmd)
					kv.lastCmdContext[args.ClientId] = OpContext{
						seqId: args.SeqId,
						reply: resp,
					}
				}

				term, isLeader := kv.rf.GetState()
				if !isLeader {
					utils.Debug(utils.DWarn, "S%d not leader, not notify", kv.me)
					kv.unlock("apply msg")
					continue
				}

				it := IndexAndTerm{msg.CommandIndex, term}
				ch, ok := kv.cmdRespChans[it]
				if !ok {
					utils.Debug(utils.DWarn, "S%d don't have channel to notify %+v", kv.me, msg)
					kv.unlock("apply msg")
					continue
				}
				ch <- resp
				kv.unlock("apply msg")
			} else {

			}
		}
	}
}

func (kv *KVServer) isDuplicate(clientId int64, seqId int64) bool {
	context, ok := kv.lastCmdContext[clientId]
	if !ok {
		return false
	}
	if seqId <= context.seqId {
		return true
	}
	return false
}
