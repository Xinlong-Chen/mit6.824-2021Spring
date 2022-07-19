package kvraft

import (
	"bytes"
	"log"

	"6.824/labgob"
	"6.824/utils"
)

const threshold int = 32

func (kv *KVServer) isNeedSnapshot() bool {
	if kv.maxraftstate != -1 && kv.rf.RaftPersistSize()+threshold > kv.maxraftstate {
		return true
	}
	return false
}

func (kv *KVServer) doSnapshot(commandIndex int) {
	utils.Debug(utils.DServer, "S%d doSnapshot", kv.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(*kv.KvMap) != nil ||
		e.Encode(kv.LastCmdContext) != nil {
		panic("server doSnapshot encode error")
	}
	// fmt.Printf("S%d doSnapshot before: %d\n", kv.me, kv.rf.RaftPersistSize())
	kv.rf.Snapshot(commandIndex, w.Bytes())
	// fmt.Printf("S%d doSnapshot after: %d\n", kv.me, kv.rf.RaftPersistSize())
}

func (kv *KVServer) setSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	utils.Debug(utils.DServer, "S%d setSnapshot", kv.me)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvMap KV
	var lastCmdContext map[int64]OpContext

	if d.Decode(&kvMap) != nil ||
		d.Decode(&lastCmdContext) != nil {
		log.Fatalf("server setSnapshot decode error\n")
	} else {
		kv.KvMap = &kvMap
		kv.LastCmdContext = lastCmdContext
	}
}
