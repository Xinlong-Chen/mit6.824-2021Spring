package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"6.824/labgob"
	"6.824/shardctrler"
)

const threshold float32 = 0.8
const snapshotLogGap int = 10

func (kv *ShardKV) snapshoter() {
	for kv.killed() == false {
		kv.mu.Lock()
		if kv.isNeedSnapshot() {
			kv.doSnapshot(kv.lastApplied)
			kv.lastSnapshot = kv.lastApplied
		}
		kv.mu.Unlock()
		time.Sleep(snapshot_gap_time)
	}
}

func (kv *ShardKV) isNeedSnapshot() bool {
	for _, shard := range kv.shards {
		if shard.Status == BePulling {
			return false
		}
	}

	if kv.maxraftstate != -1 {
		if kv.rf.RaftPersistSize() > int(threshold*float32(kv.maxraftstate)) ||
			kv.lastApplied > kv.lastSnapshot+snapshotLogGap {
			return true
		}
	}
	return false
}

func (kv *ShardKV) doSnapshot(commandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.shards) != nil ||
		e.Encode(kv.lastConfig) != nil ||
		e.Encode(kv.currentConfig) != nil {
		panic("server doSnapshot encode error")
	}
	kv.rf.Snapshot(commandIndex, w.Bytes())
}

func (kv *ShardKV) setSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shards map[int]*Shard
	var lastconfig, currentConfig shardctrler.Config

	if d.Decode(&shards) != nil ||
		d.Decode(&lastconfig) != nil ||
		d.Decode(&currentConfig) != nil {
		log.Fatalf("server setSnapshot decode error\n")
	} else {
		var str string
		for shardID, shard := range shards {
			desc := fmt.Sprintf("[%d : %+v]\n ", shardID, shard)
			str += desc
		}
		Debug(dWarn, "G%+v {S%+v} snapshot read: %+v", kv.gid, kv.me, str)
		kv.shards = shards
		kv.lastConfig = lastconfig
		kv.currentConfig = currentConfig
	}
}
