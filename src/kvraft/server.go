package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"mit6.824/src/labgob"
	"mit6.824/src/labrpc"
	"mit6.824/src/raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method string
	Key    string
	Value  string
	OpId   OpId
}

type Result struct {
	result bool
	ch     chan bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvs         map[string]string // key-value 存储
	commitRes   map[int]*Result   // applyMsg的响应通道, 返回命令是否被raft提交
	latestSeqId map[int64]int64   // 记录每个clerk最新的seqId
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Method: GET,
		Key:    args.Key,
		OpId:   args.OpId,
	}
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if _, ok := kv.commitRes[index]; !ok {
		kv.commitRes[index] = &Result{ch: make(chan bool)}
	}
	if !isLeader {
		DPrintf("[KVServer %d] is not leader\n", kv.me)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	res := kv.commitRes[index]
	kv.mu.Unlock()

	select {
	case <-res.ch:
		break
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
		return
	}

	if !res.result {
		reply.Err = ErrNoKey
		DPrintf("[KVServer %d] key: %s not found\n", kv.me, args.Key)
		return
	}

	kv.mu.Lock()
	currentTerm, isLeader := kv.rf.GetState()
	// important bug
	if isLeader && term == currentTerm {
		reply.Value = kv.kvs[args.Key]
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
	DPrintf("[KVServer %d] Get: %v\n", kv.me, kv.kvs[args.Key])
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	if args.Op != PUT && args.Op != APPEND {
		reply.Err = ErrNoKey
		return
	}

	op := Op{
		Method: args.Op,
		Key:    args.Key,
		Value:  args.Value,
		OpId:   args.OpId,
	}
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if _, ok := kv.commitRes[index]; !ok {
		kv.commitRes[index] = &Result{ch: make(chan bool)}
	}
	DPrintf("[KVServer %d] PutAppend: %v, OpId: %v\n", kv.me, op, op.OpId)
	if !isLeader {
		DPrintf("[KVServer %d] is not leader\n", kv.me)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	res := kv.commitRes[index]
	kv.mu.Unlock()

	select {
	case <-res.ch:
		break
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
		return
	}

	if !res.result {
		reply.Err = ErrNoKey
		return
	}

	kv.mu.Lock()
	currentTerm, isLeader := kv.rf.GetState()
	// important bug
	if isLeader && term == currentTerm {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
	DPrintf("[KVServer %d] PutAppend: %v\n", kv.me, kv.kvs[args.Key])
	kv.mu.Unlock()
}

func (kv *KVServer) applyMsgReceiver() {
	for applyMsg := range kv.applyCh {
		if kv.killed() {
			return
		}

		index := applyMsg.CommandIndex

		kv.mu.Lock()
		DPrintf("[KVServer %d] applyMsgReceiver: %v, OpId: %v\n", kv.me, applyMsg, applyMsg.Command.(Op).OpId)
		op := applyMsg.Command.(Op)
		opId := op.OpId
		if _, ok := kv.latestSeqId[opId.ClerkId]; !ok {
			kv.latestSeqId[opId.ClerkId] = -1
		}

		switch op.Method {
		case GET:
			{
				// important bug, don't need to check the latestSeqId in GET
				DPrintf("[KVServer %d] Get: key: %s\n", kv.me, op.Key)
				break
			}
		case PUT:
			{
				if opId.SeqId > kv.latestSeqId[opId.ClerkId] {
					DPrintf("[KVServer %d] Put: key: %s, value: %s\n", kv.me, op.Key, op.Value)
					kv.kvs[op.Key] = op.Value
				}
				break
			}
		case APPEND:
			{
				if opId.SeqId > kv.latestSeqId[opId.ClerkId] {
					DPrintf("[KVServer %d] Append: key: %s, value: %s\n", kv.me, op.Key, op.Value)
					kv.kvs[op.Key] = kv.kvs[op.Key] + op.Value
				}
				break
			}
		}

		kv.latestSeqId[opId.ClerkId] = opId.SeqId

		if _, ok := kv.commitRes[index]; ok {
			kv.commitRes[index].result = true
			close(kv.commitRes[index].ch)
			delete(kv.commitRes, index)
		}
		kv.mu.Unlock()
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.commitRes = make(map[int]*Result)
	kv.latestSeqId = make(map[int64]int64)

	go kv.applyMsgReceiver()

	DPrintf("[KVServer %d] start\n", kv.me)

	return kv
}
