package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"mit6.824/src/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId  int64
	seqId    int64
	leaderId int
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
	ck.clerkId = nrand()
	ck.seqId = 0
	ck.leaderId = 0

	DPrintf("[Clerk %d] MakeClerk, servers: %v", ck.clerkId, ck.servers)

	return ck
}

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
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	value := ""
	for {
		leader := ck.servers[ck.leaderId]

		args := &GetArgs{
			Key: key,
			OpId: OpId{
				ClerkId: ck.clerkId,
				SeqId:   ck.seqId,
			},
		}
		reply := &GetReply{}

		DPrintf("[Clerk %d] Get to leader %d, key: %s", ck.clerkId, ck.leaderId, key)
		ok := leader.Call("KVServer.Get", args, reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		if reply.Err == ErrNoKey {
			break
		} else {
			value = reply.Value
			break
		}
	}

	ck.seqId++

	return value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	for {
		leader := ck.servers[ck.leaderId]

		args := &PutAppendArgs{
			Key:   key,
			Value: value,
			Op:    op,
			OpId: OpId{
				ClerkId: ck.clerkId,
				SeqId:   ck.seqId,
			},
		}
		reply := &PutAppendReply{}

		DPrintf("[Clerk %d] PutAppend to leader %d, key: %s, value: %s, op: %s", ck.clerkId, ck.leaderId, key, value, op)
		ok := leader.Call("KVServer.PutAppend", args, reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		DPrintf("[Clerk %d] PutAppend to leader %d, key: %s, value: %s, op: %s, success", ck.clerkId, ck.leaderId, key, value, op)
		break
	}

	ck.seqId++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
