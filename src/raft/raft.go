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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State int

func (s State) String() string {
	switch s {
	case FOLLOWER:
		return "follower"
	case CANDIDATE:
		return "candidate"
	case LEADER:
		return "leader"
	}

	return "unknown"
}

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

type LogEntry struct {
	Term    int         // 该条日志的Term
	Index   int         // 该条日志的Index
	Command interface{} // 该条日志的命令
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
	state       State      // 此节点的当前状态
	stateChan   chan State // 用于通知状态变更
	currentTerm int        // 此节点的任期
	voteFor     int        // 在当前任期内，此节点将选票投给了谁。
	// 2B
	logs        []*LogEntry   // 日志
	commitIndex int           // 已经提交的日志的Index
	lastApplied int           // 已经应用的日志的Index
	nextIndex   []int         // 每个节点下一条要发送的日志的Index
	matchIndex  []int         // 每个节点已经复制的日志的Index
	applyCh     chan ApplyMsg // 向应用Commit日志的通道

	// timer
	electionTimer  *time.Timer // 选举超时计时器
	heartBeatTimer *time.Timer // 心跳的计时器
}

// 必须带锁rf.mu掉用该函数
func (rf *Raft) resetElectionTimer() {
	timeout := time.Duration(300+rand.Intn(300)) * time.Millisecond
	rf.electionTimer.Reset(timeout)
	DPrintf("[Raft %d {%d}] resetElectionTimer: %v", rf.me, rf.currentTerm, timeout)
}

// 必须带锁rf.mu掉用该函数
func (rf *Raft) resetHeartBeatTimeout() {
	timeout := time.Duration(120 * time.Millisecond)
	rf.heartBeatTimer.Reset(timeout)
	DPrintf("[Raft %d {%d}] resetHeartBeatTimeout: %v", rf.me, rf.currentTerm, timeout)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId  int // 竞选者的Id
	Term         int // 竞选者的Term
	LastLogTerm  int // 竞选者的最后一个Log的Term
	LastLogIndex int // 竞选者的最后一个Log的Index
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 节点的Term
	VoteGranted bool // 节点是否投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[Raft %d {%d}] RequestVote from %d", rf.me, rf.currentTerm, args.CandidateId)

	// 竞选者的Term更小，拒绝
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// 同Term内已经投过票了，拒绝
	if args.Term == rf.currentTerm && rf.voteFor != -1 {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 竞选者的Term更大，继续检查Log
	// 例如可能有小分区的节点加入进来，那个分区一直在不断的选举，但是没有人能够获得大多数选票，
	// 导致节点的Term很大，但是Log却很小，与当前分区的节点的Log冲突
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}

	lastLogTerm := rf.logs[len(rf.logs)-1].Term
	lastLogIndex := rf.logs[len(rf.logs)-1].Index
	// 同Term但是log长度更小，拒绝
	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		reply.VoteGranted = false
		return
	}
	// 竞选者有更大或至少相同的log，同意
	rf.voteFor = args.CandidateId
	rf.changeStateTo(FOLLOWER)

	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) election() {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	DPrintf("[Raft %d {%d}] startElection", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	resultChan := make(chan bool, len(rf.peers)) // 必须色缓冲通道，否则下一条语句会阻塞
	resultChan <- true                           // 自己给自己投了一票
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			rf.mu.Lock()
			if rf.state != CANDIDATE {
				rf.mu.Unlock()
				return
			}

			args := &RequestVoteArgs{
				CandidateId:  rf.me,
				Term:         rf.currentTerm,
				LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
				LastLogIndex: rf.logs[len(rf.logs)-1].Index,
			}
			rf.mu.Unlock()

			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if !ok {
				resultChan <- false
				return
			}

			if rf.state != CANDIDATE {
				return
			}

			if !reply.VoteGranted {
				// 可能是对方dead或者比自己Term大，
				// 如果是比自己Term大，当前节点不应该在低Term继续竞选领导人了
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.changeStateTo(FOLLOWER)
				}
				resultChan <- false
				return
			} else {
				resultChan <- true
			}
		}(i)
	}

	votesCnt := 0
	votesGrantedNum := 0
	for result := range resultChan {
		votesCnt++
		if result {
			votesGrantedNum++
		}

		rf.mu.Lock()
		if rf.state != CANDIDATE {
			rf.mu.Unlock()
			return
		}

		if votesGrantedNum > len(rf.peers)/2 {
			rf.changeStateTo(LEADER)
			rf.mu.Unlock()
			return
		} else if votesCnt >= len(rf.peers) {
			// 选举失败，重新开启新一轮选举
			rf.changeStateTo(FOLLOWER)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

type AppendEntriesArgs struct {
	LeaderId     int         // 领导者的ID
	Term         int         // 领导者的Term
	LeaderCommit int         // 领导者的已经提交的日志的Index
	Entries      []*LogEntry // 领导者的期望同步的日志
	PrevLogTerm  int         // 领导者的PrevLog的Term
	PrevLogIndex int         // 领导者的PrevLog的Index
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int // 快速回退用，term of conflicting entry
	XIndex int // 快速回退时用，term是Xterm的最早的index
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Term = -1
		reply.Success = false
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[Raft %d {%d}] AppendEntries from %d", rf.me, rf.currentTerm, args.LeaderId)

	// 竞选者的Term更小，拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 竞选者的Term更大，接受AE
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	rf.changeStateTo(FOLLOWER)

	// 该节点落后过多，Leader期望的PrevLogIndex 超过了当前节点的日志长度
	if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.logs) {
		reply.Term = rf.currentTerm
		reply.Success = false

		reply.XTerm = rf.logs[len(rf.logs)-1].Term
		reply.XIndex = len(rf.logs) - 1
		return

	}
	// 对比同一位置的Log是否与Leader的PrevLog相同
	prevLog := rf.logs[args.PrevLogIndex]
	if prevLog.Term != args.PrevLogTerm || prevLog.Index != args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		// 寻找与冲突的PrevLogTerm相同Term的第一个Index，
		// 让Leader发送其之后的所有元素
		reply.XTerm = prevLog.Term
		for i := prevLog.Index; i > 0; i-- {
			if rf.logs[i].Term != rf.logs[i-1].Term {
				reply.XIndex = i
				break
			}
		}
		return
	}
	// 从PrevLogIndex+1同步Leader的日志
	rf.logs = rf.logs[:args.PrevLogIndex+1]
	rf.logs = append(rf.logs, args.Entries...)
	rf.matchIndex[rf.me] = len(rf.logs) - 1
	rf.nextIndex[rf.me] = len(rf.logs)
	DPrintf("[Raft %d {%d}] AppendEntries success, log length: %d", rf.me, rf.currentTerm, len(rf.logs))
	// 同步Leader的commitIndex
	rf.commitIndex = args.LeaderCommit

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) heartBeat() {
	broadcast := func() {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(i int) {
				rf.mu.Lock()
				if rf.state != LEADER {
					rf.mu.Unlock()
					return
				}

				entries := make([]*LogEntry, 0)
				if rf.nextIndex[i] < len(rf.logs) {
					entries = append(entries, rf.logs[rf.nextIndex[i]:]...)
				}
				prevLog := rf.logs[rf.nextIndex[i]-1]
				args := &AppendEntriesArgs{
					LeaderId:     rf.me,
					Term:         rf.currentTerm,
					LeaderCommit: rf.commitIndex,
					Entries:      entries,
					PrevLogTerm:  prevLog.Term,
					PrevLogIndex: prevLog.Index,
				}
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}

				ok := rf.sendAppendEntries(i, args, reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != LEADER {
					return
				}

				if !reply.Success {
					if reply.Term > args.Term {
						rf.currentTerm = reply.Term
						rf.voteFor = -1
						rf.changeStateTo(FOLLOWER)
						return
					} else {
						rf.nextIndex[i] = reply.XIndex
					}
				} else {
					rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1
				}
			}(i)
		}
	}

	for !rf.killed() {
		rf.mu.Lock()
		// DPrintf("[Raft %d {%d}] heartBeat", rf.me, rf.currentTerm)
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		DPrintf("[Raft %d {%d}] heartBeat with log length: %d", rf.me, rf.currentTerm, len(rf.logs))
		rf.mu.Unlock()

		<-rf.heartBeatTimer.C
		broadcast()

		rf.mu.Lock()
		rf.resetHeartBeatTimeout()
		rf.mu.Unlock()

		rf.updateCommitIndex()
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for ; rf.commitIndex < len(rf.logs); rf.commitIndex++ {
		matchedNum := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= rf.commitIndex {
				matchedNum++
			}
		}

		if matchedNum <= len(rf.peers)/2 {
			break
		}
	}

	rf.commitIndex--
}

func (rf *Raft) applier() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex || rf.lastApplied >= len(rf.logs)-1 {
			rf.mu.Unlock()
			continue
		}

		rf.lastApplied++
		log := rf.logs[rf.lastApplied]
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      log.Command,
			CommandIndex: log.Index,
		}
		rf.mu.Unlock()

		rf.applyCh <- applyMsg
		DPrintf("[Raft %d {%d}] apply log: %v", rf.me, rf.currentTerm, log)
	}
}

func (rf *Raft) follow() {
	<-rf.electionTimer.C

	rf.mu.Lock()
	if rf.state != FOLLOWER {
		rf.mu.Unlock()
		return
	}
	rf.changeStateTo(CANDIDATE)
	rf.mu.Unlock()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.state != LEADER {
		return -1, -1, false
	}

	DPrintf("[Raft %d {%d}] Add log: %v", rf.me, rf.currentTerm, command)
	term := rf.currentTerm
	index := len(rf.logs)
	rf.logs = append(rf.logs, &LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	})
	rf.matchIndex[rf.me] = len(rf.logs) - 1
	rf.nextIndex[rf.me] = len(rf.logs)

	return index, term, true
}

func (rf *Raft) StateMachine() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		DPrintf("[Raft %d] StateMachine: %v", rf.me, state)

		switch state {
		case FOLLOWER:
			{
				go rf.follow()
				break
			}
		case CANDIDATE:
			{
				go rf.election()
				break
			}
		case LEADER:
			{
				go rf.heartBeat()
				break
			}
		}

		rf.waitStateChange()
	}
}

// 必须带锁rf.mu掉用该函数
func (rf *Raft) changeStateTo(state State) {
	// 如果想要变成Leader，但是当前状态不是Candidate，不允许
	rf.stateChan <- state
}

// 不带锁rf.mu调用该函数
// 该函数会阻塞，直到有其他goroutine调用changeStateTo
func (rf *Raft) waitStateChange() {
	state := <-rf.stateChan

	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch rf.state {
	case FOLLOWER:
		{
			if state == FOLLOWER {
				rf.resetElectionTimer()
			} else if state == CANDIDATE {
				rf.currentTerm++
				rf.voteFor = rf.me
			} else {
				panic(fmt.Sprintf("Invalid state change from %v to %v", rf.state, state))
			}
			break
		}
	case CANDIDATE:
		{
			if state == FOLLOWER {
				rf.voteFor = -1
				rf.resetElectionTimer()
			} else if state == LEADER {
				rf.voteFor = -1
				for peerIdx := range rf.peers {
					rf.nextIndex[peerIdx] = len(rf.logs)
					rf.matchIndex[peerIdx] = 0
				}
				rf.matchIndex[rf.me] = len(rf.logs) - 1
				rf.resetHeartBeatTimeout()
			} else {
				panic(fmt.Sprintf("Invalid state change from %v to %v", rf.state, state))
			}

			break
		}
	case LEADER:
		{
			if state == FOLLOWER {
				rf.resetElectionTimer()
			} else {
				panic(fmt.Sprintf("Invalid state change from %v to %v", rf.state, state))
			}

			break
		}
	}

	rf.state = state
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
	rf.stateChan = make(chan State, 1)
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.electionTimer = time.NewTimer(0)
	rf.resetElectionTimer()
	rf.heartBeatTimer = time.NewTimer(0)
	rf.resetHeartBeatTimeout()

	rf.logs = append(rf.logs, &LogEntry{Term: -1, Index: 0, Command: nil})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("Initiate raft server %d as %v", rf.me, rf.state)

	go rf.StateMachine()
	// Your code here (2B).
	go rf.applier()

	return rf
}
