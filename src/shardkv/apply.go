package shardkv

import (
	"time"

	"6.824/shardctrler"
)

func (kv *ShardKV) isDuplicate(shardId int, clientId int64, seqId int64) bool {
	context, ok := kv.shards[shardId].LastCmdContext[clientId]
	if !ok {
		return false
	}
	if seqId <= context.SeqId {
		return true
	}
	return false
}

func (kv *ShardKV) applier() {
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
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
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex

				var resp OpResp
				command := msg.Command.(Command)
				switch command.Op {
				case Operation:
					cmd := command.Data.(CmdArgs)
					resp = *kv.applyOperation(&msg, &cmd)
				case Configuration:
					nextConfig := command.Data.(shardctrler.Config)
					resp = *kv.applyConfiguration(&nextConfig)
				case InsertShards:
					insertResp := command.Data.(PullDataReply)
					resp = *kv.applyInsertShards(&insertResp)
				case DeleteShards:
					deleteResp := command.Data.(PullDataArgs)
					resp = *kv.applyDeleteShards(&deleteResp)
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
					case <-time.After(10 * time.Millisecond):
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
