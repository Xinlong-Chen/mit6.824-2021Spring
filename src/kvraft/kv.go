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
