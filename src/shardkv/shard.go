package shardkv

type ShardStatus uint8

const (
	Serving ShardStatus = iota
	Pulling
	BePulling
	GCing
)

type Shard struct {
	KV     map[string]string
	Status ShardStatus
}

func NewShard(status ShardStatus) *Shard {
	return &Shard{make(map[string]string), status}
}

func (shard *Shard) Get(key string) (string, Err) {
	if value, ok := shard.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (shard *Shard) Put(key, value string) Err {
	shard.KV[key] = value
	return OK
}

func (shard *Shard) Append(key, value string) Err {
	shard.KV[key] += value
	return OK
}

func (shard *Shard) deepCopy() map[string]string {
	newShard := make(map[string]string)
	for k, v := range shard.KV {
		newShard[k] = v
	}
	return newShard
}

func (kv *ShardKV) Opt(cmd *CmdArgs, shardID int) (string, Err) {
	shard := kv.shards[shardID]

	switch cmd.OpType {
	case OpGet:
		value, err := shard.Get(cmd.Key)
		return value, err
	case OpPut:
		err := shard.Put(cmd.Key, cmd.Value)
		return "", err
	case OpAppend:
		err := shard.Append(cmd.Key, cmd.Value)
		return "", err
	default:
		return "", OK
	}
}
