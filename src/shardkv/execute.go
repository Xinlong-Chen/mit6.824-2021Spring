package shardkv

import "time"

func (kv *ShardKV) Execute(cmd Command, reply *OpResp) {
	index, term, is_leader := kv.rf.Start(cmd)
	if !is_leader {
		reply.Value, reply.Err = "", ErrWrongLeader
		return
	}

	kv.mu.Lock()
	it := IndexAndTerm{index, term}
	ch := make(chan OpResp, 1)
	kv.cmdRespChans[it] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		// close(kv.cmdRespChans[index])
		delete(kv.cmdRespChans, it)
		kv.mu.Unlock()
		close(ch)
	}()

	t := time.NewTimer(cmd_timeout)
	defer t.Stop()

	for {
		kv.mu.Lock()
		select {
		case resp := <-ch:
			reply.Value, reply.Err = resp.Value, resp.Err
			kv.mu.Unlock()
			return
		case <-t.C:
		priority:
			for {
				select {
				case resp := <-ch:
					reply.Value, reply.Err = resp.Value, resp.Err
					kv.mu.Unlock()
					return
				default:
					break priority
				}
			}
			reply.Value, reply.Err = "", ErrTimeout
			kv.mu.Unlock()
			return
		default:
			kv.mu.Unlock()
			time.Sleep(gap_time)
		}
	}
}
