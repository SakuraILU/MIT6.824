package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"mit6.824/src/labgob"
	"mit6.824/src/labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	// Log Command 2B
	CommandValid bool
	Command      interface{}
	CommandTerm  int
	CommandIndex int
	// Snapshot 2D
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2A
	state             State         // 此节点的当前状态
	currentTerm       int           // 此节点的任期
	voteFor           int           // 在当前任期内，此节点将选票投给了谁。
	stateCond         *sync.Cond    // 用于等待选举的条件变量
	sendRPCTimeout    time.Duration // 发送AppendEntries RPC的超时时间
	electionTime      time.Time     // 触发选举的时刻
	heartBeatDuration time.Duration // 心跳周期
	heartBeatConds    []*sync.Cond  // 用于等待心跳的条件变量
	// 2B
	logs        *Logs         // 日志
	commitIndex int           // 已经提交的日志的Index
	lastApplied int           // 已经应用的日志的Index
	nextIndex   []int         // 每个节点下一条要发送的日志的Index
	matchIndex  []int         // 每个节点已经复制的日志的Index
	applyCh     chan ApplyMsg // 向应用Commit日志的通道
	applyCond   *sync.Cond    // 用于等待应用日志的条件变量
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.lockState()
	defer rf.unlockState()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 带锁rf.mu调用此函数
func (rf *Raft) persist() {
	// Your code here (2C).
	buffer := &bytes.Buffer{}
	encoder := labgob.NewEncoder(buffer)

	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.voteFor)
	encoder.Encode(rf.logs)

	data := buffer.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
// 带锁rf.mu调用此函数,除了单线程启动时
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var term int
	var voteFor int
	logs := &Logs{}
	if decoder.Decode(&term) != nil ||
		decoder.Decode(&voteFor) != nil ||
		decoder.Decode(logs) != nil {
		panic("Read Persist error")
	} else {
		rf.currentTerm = term
		rf.voteFor = voteFor
		rf.logs = logs
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.lockState()
	defer rf.unlockState()
	if rf.killed() || rf.state != LEADER {
		return -1, -1, false
	}

	term := rf.currentTerm
	index := rf.logs.getLogLength()
	DPrintf("[Raft %d {%d}] Add log: %v in %d", rf.me, rf.currentTerm, command, index)
	rf.logs.appendLog(&LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	})

	rf.matchIndex[rf.me] = rf.logs.getLogLength() - 1
	rf.nextIndex[rf.me] = rf.logs.getLogLength()

	rf.heartBeat()

	return index, term, true
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.lockState()
	defer rf.unlockState()
	DPrintf("[Raft %d {%d}] Snapshot from %d", rf.me, rf.currentTerm, index)
	if index <= rf.logs.getLastIncludedIndex() {
		return
	} else if index >= rf.logs.getLogLength() {
		return
	} else {
		rf.logs.truncLogBefore(index)
	}
	rf.persister.SaveStateAndSnapshot(rf.statePersistData(), snapshot)
	// important bug, find the bug in updateCommitIndex: invalid getLog(i)
	// rf.lastApplied = max(rf.logs.getLastIncludedIndex(), rf.lastApplied)
	// rf.commitIndex = max(rf.logs.getLastIncludedIndex(), rf.commitIndex)
	// rf.setLastAppliedSafe(rf.logs.getLastIncludedIndex())
	rf.setCommitIndexSafe(rf.logs.getLastIncludedIndex())
	rf.setLastAppliedSafe(rf.logs.getLastIncludedIndex())
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.stateCond = sync.NewCond(&rf.mu)
	rf.heartBeatDuration = heartBeatDuration
	rf.sendRPCTimeout = 200 * time.Millisecond
	rf.heartBeatConds = make([]*sync.Cond, len(rf.peers))
	for i := range rf.peers {
		rf.heartBeatConds[i] = sync.NewCond(&rf.mu)
	}
	rf.logs = MakeLogs(0, 0)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.logs.getLastIncludedIndex() != 0 {
		// send snapshot to applyCh
		applyMsg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.persister.ReadSnapshot(),
			SnapshotTerm:  rf.logs.getLastIncludedTerm(),
			SnapshotIndex: rf.logs.getLastIncludedIndex(),
		}
		go func() {
			rf.applyCh <- applyMsg
		}()
	}
	rf.commitIndex = rf.logs.getLastIncludedIndex()
	rf.lastApplied = rf.logs.getLastIncludedIndex()

	DPrintf("Initiate raft server %d as %v", rf.me, rf.state)

	rf.resetElectionTimer()
	// Your code here (2B).
	go rf.electionHandler()
	go rf.heartBeatHandler()
	go rf.applier()

	return rf
}
