package shardctrler

import (
	"time"
)

func (sc *ShardCtrler) applier() {
	for sc.killed() == false {
		select {
		case msg := <-sc.applyCh:
			Debug(dServer, "S%d apply msg: %+v", sc.me, msg)
			if msg.CommandValid {
				sc.mu.Lock()

				if msg.CommandIndex <= sc.lastApplied {
					Debug(dWarn, "S%d out time apply(%d <= %d): %+v", sc.me, msg.CommandIndex, sc.lastApplied, msg)
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = msg.CommandIndex

				var resp OpResp
				cmd := msg.Command.(Op)

				if cmd.Op != OpQuery && sc.isDuplicate(cmd.ClientId, cmd.SeqId) {
					context := sc.LastCmdContext[cmd.ClientId]
					resp = context.Reply
				} else {
					resp.Config, resp.Err = sc.configs.Opt(cmd)
					sc.LastCmdContext[cmd.ClientId] = OpContext{
						SeqId: cmd.SeqId,
						Reply: resp,
					}
				}

				term, isLeader := sc.rf.GetState()

				if !isLeader || term != msg.CommandTerm {
					sc.mu.Unlock()
					continue
				}

				it := IndexAndTerm{msg.CommandIndex, term}
				ch, ok := sc.cmdRespChans[it]
				if ok {
					select {
					case ch <- resp:
					case <-time.After(10 * time.Millisecond):
					}
				}

				sc.mu.Unlock()
			} else {
				// ignore
			}
		default:
			time.Sleep(gap_time)
		}
	}
}

func (sc *ShardCtrler) isDuplicate(clientId int64, seqId int64) bool {
	context, ok := sc.LastCmdContext[clientId]
	if !ok {
		return false
	}
	if seqId <= context.SeqId {
		return true
	}
	return false
}
