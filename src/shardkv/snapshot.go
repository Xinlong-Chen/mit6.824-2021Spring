package shardkv

import (
	"bytes"
	"log"
	"time"

	"6.824/labgob"
)

const threshold float32 = 0.8
const snapshotLogGap int = 3

func (kv *ShardKV) snapshoter() {
	for kv.killed() == false {
		kv.mu.Lock()
		if kv.isNeedSnapshot() && kv.lastApplied > kv.lastSnapshot+snapshotLogGap {
			kv.doSnapshot(kv.lastApplied)
			kv.lastSnapshot = kv.lastApplied
		}
		kv.mu.Unlock()
		time.Sleep(snapshot_gap_time)
	}
}

func (kv *ShardKV) isNeedSnapshot() bool {
	if kv.maxraftstate != -1 && kv.rf.RaftPersistSize() > int(threshold*float32(kv.maxraftstate)) {
		return true
	}
	return false
}

func (kv *ShardKV) doSnapshot(commandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.shards) != nil {
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

	if d.Decode(&shards) != nil {
		log.Fatalf("server setSnapshot decode error\n")
	} else {
		kv.shards = shards
	}
}
