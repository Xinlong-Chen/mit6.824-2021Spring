package kvraft

import "6.824/utils"
import "time"

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
				cmd := msg.Command.(Op)

				if cmd.OpType != OpGet && kv.isDuplicate(cmd.ClientId, cmd.SeqId) {
					context := kv.LastCmdContext[cmd.ClientId]
					resp = context.Reply
				} else {
					resp.Value, resp.Err = kv.Opt(cmd)
					kv.LastCmdContext[cmd.ClientId] = OpContext{
						SeqId: cmd.SeqId,
						Reply: resp,
					}
				}

				term, isLeader := kv.rf.GetState()

				if !isLeader || term != msg.CommandTerm {
					kv.mu.Unlock()
					continue
				}

				it := IndexAndTerm{msg.CommandIndex, term}
				ch, ok := kv.cmdRespChans[it]
				if ok {
					select {
					case ch <- resp:
					case <- time.After(10 * time.Millisecond):
					}
				}

				kv.mu.Unlock()
			} else {
				// ignore
			}
		default:
			time.Sleep(gap_time)
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
