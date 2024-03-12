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
	state       State      // 此节点的当前状态
	stateChan   chan State // 用于通知状态变更
	currentTerm int        // 此节点的任期
	voteFor     int        // 在当前任期内，此节点将选票投给了谁。

	// timer
	electionTimer  *time.Timer // 选举超时计时器
	heartBeatTimer *time.Timer // 心跳的计时器
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
	CandidateId int // 竞选者的Id
	Term        int // 竞选者的Term
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

	reply.Term = rf.currentTerm
	// 竞选者的Term更小，拒绝
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	// 同Term内已经投过票了，拒绝
	if args.Term == rf.currentTerm && rf.voteFor != -1 {
		reply.VoteGranted = false
		return
	}
	// TODO：同Term但是log长度更小，拒绝
	// TODO: 同Term且log大于等于，同意

	// 竞选者的Term更大，同意
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = args.CandidateId
		rf.changeStateTo(FOLLOWER)

		reply.VoteGranted = true
		reply.Term = rf.currentTerm
	}
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
			args := &RequestVoteArgs{
				CandidateId: rf.me,
				Term:        rf.currentTerm,
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

			if !reply.VoteGranted {
				// 可能是对方dead或者比自己Term大，
				// 如果是比自己Term大，当前节点不应该在低Term继续竞选领导人了
				if reply.Term > rf.currentTerm {
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
			rf.changeStateTo(CANDIDATE)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

type AppendEntriesArgs struct {
	LeaderId int // 领导者的ID
	Term     int // 领导者的Term
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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

	// TODO：同Term但是log长度更小，拒绝
	// TODO: 同Term且log大于等于，同意

	// 竞选者的Term更大，接受AE
	// 收到心跳，重置计时器
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}

	rf.changeStateTo(FOLLOWER)

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
	for {
		rf.mu.Lock()
		// DPrintf("[Raft %d {%d}] heartBeat", rf.me, rf.currentTerm)
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		<-rf.heartBeatTimer.C

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

				args := &AppendEntriesArgs{
					LeaderId: rf.me,
					Term:     rf.currentTerm,
				}
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}

				ok := rf.sendAppendEntries(i, args, reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > args.Term {
					rf.currentTerm = reply.Term
					rf.voteFor = -1
					rf.resetElectionTimer()
					rf.changeStateTo(FOLLOWER)
				}
			}(i)
		}

		rf.mu.Lock()
		rf.resetHeartBeatTimeout()
		rf.mu.Unlock()
	}
}

func (rf *Raft) follow() {
	<-rf.electionTimer.C

	rf.mu.Lock()
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
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
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

				rf.waitStateChange()
				break
			}
		case CANDIDATE:
			{
				go rf.election()

				rf.waitStateChange()
				break
			}
		case LEADER:
			{
				go rf.heartBeat()

				rf.waitStateChange()
				break
			}
		}
	}
}

// 必须带锁rf.mu掉用该函数
func (rf *Raft) changeStateTo(state State) {
	DPrintf("[Raft %d] change state %v -> %v", rf.me, rf.state, state)
	rf.state = state
	switch rf.state {
	case FOLLOWER:
		{
			rf.resetElectionTimer()
			break
		}
	case CANDIDATE:
		{
			rf.resetElectionTimer()
			rf.currentTerm++
			rf.voteFor = rf.me
			break
		}
	case LEADER:
		{
			rf.resetHeartBeatTimeout()
			rf.voteFor = -1
			break
		}
	}
	rf.stateChan <- rf.state
}

// 不带锁rf.mu调用该函数
func (rf *Raft) waitStateChange() {
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	// read all previous values from the channel
	// only CANDIDATE can be the same after change...otherwise, still wait until the state change
	for {
		nextState := <-rf.stateChan
		if nextState == CANDIDATE || nextState != state {
			break
		}
	}
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("Initiate raft server %d as %v", rf.me, rf.state)

	go rf.StateMachine()
	// Your code here (2B).

	return rf
}
