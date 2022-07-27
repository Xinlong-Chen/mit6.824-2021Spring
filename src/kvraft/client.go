package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	seqId    int64
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) sendCmd(key string, value string, OpType OPType) string {
	ck.seqId += 1
	args := CmdArgs{
		SeqId:    ck.seqId,
		ClientId: ck.clientId,
		Key:      key,
		Value:    value,
		OpType:   OpType,
	}

	t0 := time.Now()
	for time.Since(t0).Seconds() < 10 {
		reply := CmdReply{}

		ok := ck.servers[ck.leaderId].Call("KVServer.Command", &args, &reply)

		if !ok {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(retry_timeout)
			continue
		}

		if reply.Err == OK {
			return reply.Value
		} else if reply.Err == ErrNoKey {
			return ""
		}

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(retry_timeout)
	}
	panic("10s not reply")
	return ""
}

func (ck *Clerk) Get(key string) string {
	return ck.sendCmd(key, "", OpGet)
}

func (ck *Clerk) Put(key string, value string) {
	ck.sendCmd(key, value, OpPut)
}

func (ck *Clerk) Append(key string, value string) {
	ck.sendCmd(key, value, OpAppend)
}
