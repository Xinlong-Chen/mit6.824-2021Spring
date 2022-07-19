package kvraft

import "6.824/utils"

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			utils.Debug(utils.DServer, "S%d apply msg: %+v", kv.me, msg)
			if msg.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.setSnapshot(msg.Snapshot)
					kv.lastApplied = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			} else if msg.CommandValid {

				kv.mu.Lock()

				if msg.CommandIndex <= kv.lastApplied {
					utils.Debug(utils.DWarn, "S%d out time apply(%d <= %d): %+v", kv.me, msg.CommandIndex, kv.lastApplied, msg)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex

				var resp OpResp
				args := msg.Command.(CmdArgs)

				if args.Cmd.OpType != OpGet && kv.isDuplicate(args.ClientId, args.SeqId) {
					context := kv.LastCmdContext[args.ClientId]
					resp = context.Reply
				} else {
					resp.Value, resp.Err = kv.Opt(args.Cmd)
					kv.LastCmdContext[args.ClientId] = OpContext{
						SeqId: args.SeqId,
						Reply: resp,
					}
				}

				term, isLeader := kv.rf.GetState()
				if isLeader {
					it := IndexAndTerm{msg.CommandIndex, term}
					ch, ok := kv.cmdRespChans[it]
					if !ok {
						utils.Debug(utils.DWarn, "S%d don't have channel to notify %+v", kv.me, msg)
						kv.mu.Unlock()
						continue
					}
					ch <- resp
				}

				if kv.isNeedSnapshot() {
					kv.doSnapshot(msg.CommandIndex)
				}

				kv.mu.Unlock()
			} else {
				// ignore
			}
		}
	}
}

func (kv *KVServer) isDuplicate(clientId int64, seqId int64) bool {
	context, ok := kv.LastCmdContext[clientId]
	if !ok {
		return false
	}
	if seqId <= context.SeqId {
		return true
	}
	return false
}
