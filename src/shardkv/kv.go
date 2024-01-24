package shardkv

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key string, value string) Err
	Append(key string, value string) Err
	Migrate(shardId int, shard map[string]string) Err
	Copy(shardId int) map[string]string
	Remove(shardId int) Err
}

type KV struct {
	K2V map[int]map[string]string
}

func (kv *KV) Get(key string) (string, Err) {
	value, ok := kv.K2V[key2shard(key)][key]
	if ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (kv *KV) Put(key string, value string) Err {
	if len(kv.K2V[key2shard(key)]) == 0 {
		kv.K2V[key2shard(key)] = make(map[string]string)
	}
	kv.K2V[key2shard(key)][key] = value
	return OK
}

func (kv *KV) Append(key string, value string) Err {
	kv.K2V[key2shard(key)][key] += value
	return OK
}

func (kv *KV) Migrate(shardId int, shard map[string]string) Err {
	delete(kv.K2V, shardId)
	kv.K2V[shardId] = make(map[string]string)
	for k, v := range shard {
		kv.K2V[shardId][k] = v
	}
	return OK
}

func (kv *KV) Copy(shardId int) map[string]string {
	res := make(map[string]string)
	for k, v := range kv.K2V[shardId] {
		res[k] = v
	}
	return res
}

func (kv *KV) Remove(shardId int) Err {
	delete(kv.K2V, shardId)
	return OK
}
