package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"mit6.824/src/labgob"
	"mit6.824/src/labrpc"
	"mit6.824/src/raft"
)

const Debug = 0

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
	Method  string
	Key     string
	Value   string
	ClerkId int64
	SeqId   int64
}

type Result struct {
	result bool
	value  string
	ch     chan bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int             // snapshot if log grows this big
	persister    *raft.Persister // snapshot persister

	// Your definitions here.
	kvs         map[string]string // key-value 存储
	commitRes   map[int]*Result   // applyMsg的响应通道, 返回命令是否被raft提交
	latestSeqId map[int64]int64   // 记录每个clerk最新的seqId
	lastApplied int               // 最后一次apply的index
}

var waitDuration = 300 * time.Millisecond

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Method:  GET,
		Key:     args.Key,
		ClerkId: args.ClerkId,
		SeqId:   args.SeqId,
	}
	res, err := kv.executeCommand(op)
	if err == OK {
		reply.Value = res.value
		reply.Err = OK
	} else {
		reply.Err = err
	}
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
		Method:  args.Op,
		Key:     args.Key,
		Value:   args.Value,
		ClerkId: args.ClerkId,
		SeqId:   args.SeqId,
	}
	_, err := kv.executeCommand(op)
	if err == OK {
		reply.Err = OK
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) executeCommand(op Op) (res *Result, err Err) {
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("[KVServer %d] is not leader\n", kv.me)
		err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.commitRes[index]; !ok {
		kv.commitRes[index] = &Result{ch: make(chan bool)}
	}
	res = kv.commitRes[index]
	kv.mu.Unlock()

	select {
	case <-res.ch:
		break
	case <-time.After(waitDuration):
		err = ErrWrongLeader
		return
	}

	if !res.result {
		err = ErrNoKey
		DPrintf("[KVServer %d] key: %s not found\n", kv.me, op.Key)
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	currentTerm, isLeader := kv.rf.GetState()
	// important bug
	// 注意等待了一阵子，期间Leader可能变了，以至于请求过时
	// 应该保证RPC在请求过程中，当前Leader状态不变。。不能变成FOLLOWER或者新的Leader等
	if isLeader && term == currentTerm {
		err = OK
	} else {
		err = ErrWrongLeader
	}
	DPrintf("[KVServer %d] method: %v key: %s value: %s res: %v\n", kv.me, op.Method, op.Key, op.Value, res)

	return res, err
}

func (kv *KVServer) applyMsgReceiver() {
	for applyMsg := range kv.applyCh {
		if kv.killed() {
			return
		}

		kv.mu.Lock()

		if applyMsg.CommandValid {
			// command
			index := applyMsg.CommandIndex

			DPrintf("[KVServer %d] applyMsgReceiver: %v, ClerkId: %d, SeqId: %d\n", kv.me, applyMsg, applyMsg.Command.(Op).ClerkId, applyMsg.Command.(Op).SeqId)
			op := applyMsg.Command.(Op)
			if _, ok := kv.latestSeqId[op.ClerkId]; !ok {
				kv.latestSeqId[op.ClerkId] = -1
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
					if op.SeqId > kv.latestSeqId[op.ClerkId] {
						DPrintf("[KVServer %d] Put: key: %s, value: %s\n", kv.me, op.Key, op.Value)
						kv.kvs[op.Key] = op.Value
					}
					break
				}
			case APPEND:
				{
					if op.SeqId > kv.latestSeqId[op.ClerkId] {
						DPrintf("[KVServer %d] Append: key: %s, value: %s\n", kv.me, op.Key, op.Value)
						kv.kvs[op.Key] = kv.kvs[op.Key] + op.Value
					}
					break
				}
			}

			if _, ok := kv.commitRes[index]; ok {
				kv.commitRes[index].result = true
				kv.commitRes[index].value = kv.kvs[op.Key]
				close(kv.commitRes[index].ch)
				delete(kv.commitRes, index)
			}

			kv.latestSeqId[op.ClerkId] = op.SeqId
			kv.lastApplied = index

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				DPrintf("[KVServer %d] snapshot\n when raftStateSize: %d >= maxraftstate: %d", kv.me, kv.persister.RaftStateSize(), kv.maxraftstate)
				DPrintf("[KVServer %d] install snapshot at index: %d\n", kv.me, kv.lastApplied)
				snapshot := kv.encodeSnapshot()
				kv.rf.Snapshot(kv.lastApplied, snapshot)
			}

		} else {
			// snapshot
			DPrintf("[KVServer %d] snapshot: %v\n", kv.me, applyMsg)
			// snapshot applyMsg.Snapshot
			snapshot := applyMsg.Snapshot
			snapshotIndex := applyMsg.SnapshotIndex
			if snapshotIndex > kv.lastApplied {
				// important bug, don't need to check the latestSeqId in snapshot
				kv.decodeSnapshot(snapshot)
				kv.lastApplied = applyMsg.SnapshotIndex
			}
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) encodeSnapshot() []byte {
	buffer := &bytes.Buffer{}
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(kv.kvs)
	encoder.Encode(kv.latestSeqId)
	data := buffer.Bytes()
	return data
}

func (kv *KVServer) decodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var kvs map[string]string
	var latestSeqId map[int64]int64
	if decoder.Decode(&kvs) != nil ||
		decoder.Decode(&latestSeqId) != nil {
		panic("Decode Snapshot error")
	} else {
		kv.kvs = kvs
		kv.latestSeqId = latestSeqId
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
	kv.persister = persister

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
