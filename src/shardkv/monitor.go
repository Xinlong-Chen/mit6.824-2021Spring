package shardkv

import "time"

const (
	ConfigureMonitorTimeout time.Duration = time.Duration(10) * time.Millisecond
)

func (kv *ShardKV) startMonitor() {
	go kv.monitor(kv.configureAction, ConfigureMonitorTimeout)
}

func (kv *ShardKV) monitor(action func(), timeout time.Duration) {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}
