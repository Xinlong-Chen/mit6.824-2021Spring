package kvraft

type KV struct {
	Kvmap map[string]string
}

func NewKV() *KV {
	return &KV{make(map[string]string)}
}

func (kv *KV) Put(key string, value string) Err {
	kv.Kvmap[key] = value
	return OK
}

func (kv *KV) Append(key string, value string) Err {
	if value_ori, ok := kv.Kvmap[key]; ok {
		kv.Kvmap[key] = value_ori + value
		return OK
	}
	kv.Kvmap[key] = value
	return OK
}

func (kv *KV) Get(key string) (string, Err) {
	if value, ok := kv.Kvmap[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (kv *KVServer) Opt(cmd Op) (string, Err) {
	switch cmd.OpType {
	case OpGet:
		value, err := kv.KvMap.Get(cmd.Key)
		return value, err
	case OpPut:
		err := kv.KvMap.Put(cmd.Key, cmd.Value)
		return "", err
	case OpAppend:
		err := kv.KvMap.Append(cmd.Key, cmd.Value)
		return "", err
	default:
		return "", OK
	}
}
