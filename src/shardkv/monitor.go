package shardkv

import "time"

const (
	ConfigureMonitorTimeout        time.Duration = time.Duration(50) * time.Millisecond
	MigrationMonitorTimeout        time.Duration = time.Duration(50) * time.Millisecond
	GCMonitorTimeout               time.Duration = time.Duration(50) * time.Millisecond
	checkEntryInCurrentTermTimeout time.Duration = time.Duration(100) * time.Millisecond
)

func (kv *ShardKV) startMonitor() {
	go kv.monitor(kv.configureAction, ConfigureMonitorTimeout)
	go kv.monitor(kv.migrationAction, MigrationMonitorTimeout)
	go kv.monitor(kv.gcAction, GCMonitorTimeout)
	go kv.monitor(kv.checkEntryInCurrentTermAction, checkEntryInCurrentTermTimeout)
}

func (kv *ShardKV) monitor(action func(), timeout time.Duration) {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}
