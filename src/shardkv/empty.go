package shardkv

func (kv *ShardKV) checkEntryInCurrentTermAction() {
	if !kv.rf.HasLogInCurrentTerm() {
		kv.Execute(NewEmptyEntryCommand(), &OpResp{})
	}
}

func (kv *ShardKV) applyEmptyEntry() *OpResp {
	return &OpResp{OK, ""}
}
