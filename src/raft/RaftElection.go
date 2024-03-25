package raft

import (
	"math/rand"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId  int // 竞选者的Id
	Term         int // 竞选者的Term
	LastLogTerm  int // 竞选者的最后一个Log的Term
	LastLogIndex int // 竞选者的最后一个Log的Index
}

var electionTimeout = 400

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
	rf.lockState()
	defer rf.unlockState()

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

	// CANDIDATE的Term更大，需要变成FOLLOWER
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.changeStateTo(FOLLOWER)
	}

	// 竞选者的Term更大，继续检查Log，节点只会投给Log更大的Candidate！
	// 例如可能有小分区的节点加入进来，那个分区一直在不断的选举，但是没有人能够获得大多数选票，
	// 导致节点的Term很大，但是Log却很小，与当前分区的节点的Log冲突
	lastLogTerm := rf.logs.getLastTerm()
	lastLogIndex := rf.logs.getLastIndex()
	// log长度更小，拒绝
	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 竞选者有更大或至少相同的log，同意投票
	rf.resetElectionTimer()

	rf.voteFor = args.CandidateId

	reply.Term = rf.currentTerm
	reply.VoteGranted = true
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

func (rf *Raft) electionHandler() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.lockState()
		for rf.state == LEADER {
			rf.stateCond.Wait()
		}

		if time.Now().After(rf.electionTime) {
			rf.changeStateTo(CANDIDATE)
			rf.resetElectionTimer()
			rf.unlockState()

			go rf.election()

			rf.lockState()
		}
		rf.unlockState()
	}
}

func (rf *Raft) election() {
	rf.lockState()
	args := &RequestVoteArgs{
		CandidateId:  rf.me,
		Term:         rf.currentTerm,
		LastLogTerm:  rf.logs.getLastTerm(),
		LastLogIndex: rf.logs.getLastIndex(),
	}
	DPrintf("[Raft %d {%d}] startElection", rf.me, rf.currentTerm)
	rf.unlockState()
	resultChan := make(chan bool, len(rf.peers)) // 必须色缓冲通道，否则下一条语句会阻塞
	resultChan <- true                           // 自己给自己投了一票
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			rf.lockState()
			if rf.state != CANDIDATE {
				rf.unlockState()
				return
			}
			rf.unlockState()

			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, reply)

			rf.lockState()
			defer rf.unlockState()
			// important bug
			// 过时的RequestVote RPC，不用理会
			if !rf.stateUnchanged(CANDIDATE, args.Term) {
				return
			}

			if !ok {
				resultChan <- false
				return
			}
			if !reply.VoteGranted {
				// 可能是对方dead或者比自己Term大，
				// 如果是比自己Term大，当前节点不应该在低Term继续竞选领导人了
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.voteFor = -1
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

		rf.lockState()
		// important bug
		// 发送RequestVote等的时候时是放了锁的！这时候有可能被高Term改变currentTerm，
		// 但恰好该节点又变成了CANDIDATE... 前后两轮的election同时进行，这肯定不行啊，
		// 老的轮次需要被结束，所以需要同时检查[state + term]是否和进入时一致。
		// 几个RPC Call里的其他几处类似的地方同理
		if !rf.stateUnchanged(CANDIDATE, args.Term) {
			rf.unlockState()
			return
		}

		if votesGrantedNum > len(rf.peers)/2 {
			rf.changeStateTo(LEADER)
			rf.heartBeat()
			rf.unlockState()
			return
		} else if votesCnt >= len(rf.peers) {
			// 选举失败，直接返回。等待后续超时后再次进行选举
			rf.unlockState()
			return
		}
		rf.unlockState()
	}
}

// 必须带锁rf.mu掉用该函数
func (rf *Raft) resetElectionTimer() {
	timeout := time.Duration(electionTimeout+rand.Intn(int(electionTimeout))) * time.Millisecond
	rf.electionTime = time.Now().Add(timeout)
	DPrintf("[Raft %d {%d}] resetElectionTimer: %v", rf.me, rf.currentTerm, timeout)
}
