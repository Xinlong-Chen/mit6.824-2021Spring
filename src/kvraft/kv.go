package kvraft

type KV struct {
	kvmap map[string]string
}

func NewKV() *KV {
	return &KV{make(map[string]string)}
}

func (kv *KV) Put(key string, value string) Err {
	kv.kvmap[key] = value
	return OK
}

func (kv *KV) Append(key string, value string) Err {
	if value_ori, ok := kv.kvmap[key]; ok {
		kv.kvmap[key] = value_ori + value
		return OK
	}
	kv.kvmap[key] = value
	return OK
}

func (kv *KV) Get(key string) (string, Err) {
	if value, ok := kv.kvmap[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (kv *KVServer) Opt(cmd Op) (string, Err) {
	switch cmd.OpType {
	case OpGet:
		value, err := kv.kvMap.Get(cmd.Key)
		return value, err
	case OpPut:
		err := kv.kvMap.Put(cmd.Key, cmd.Value)
		return "", err
	case OpAppend:
		err := kv.kvMap.Append(cmd.Key, cmd.Value)
		return "", err
	default:
		return "", OK
	}
}
