package raft

import "time"

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

var heartBeatDuration = 50

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lockState()
	defer rf.unlockState()

	DPrintf("[Raft %d {%d}] AppendEntries from %d", rf.me, rf.currentTerm, args.LeaderId)

	// 竞选者的Term更小，拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 接受AE, 重置计时器
	rf.resetElectionTimer()
	// 竞选者的Term更大，需要变成FOLLOWER
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.changeStateTo(FOLLOWER)
	}

	// 具体投票与否还是得看Logs
	// 该节点落后过多，Leader期望的PrevLogIndex 超过了当前节点的日志长度
	if args.PrevLogIndex >= len(rf.logs) {
		reply.Term = rf.currentTerm
		reply.Success = false

		reply.XTerm = -1
		reply.XIndex = len(rf.logs)
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
			if rf.logs[i-1].Term != prevLog.Term {
				reply.XIndex = i
				break
			}
		}
		return
	}
	// 从PrevLogIndex+1同步Leader的日志
	rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
	DPrintf("[Raft %d {%d}] AppendEntries success, log length: %d", rf.me, rf.currentTerm, len(rf.logs))
	// 同步Leader的commitIndex
	rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) heartBeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			rf.lockState()
			if rf.state != LEADER {
				rf.unlockState()
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
			rf.unlockState()

			reply := &AppendEntriesReply{}

			ok := rf.sendAppendEntries(i, args, reply)
			if !ok {
				return
			}

			rf.lockState()
			defer rf.unlockState()
			// important bug
			if !rf.stateUnchanged(LEADER, args.Term) {
				return
			}

			if reply.Success {
				rf.matchIndex[i] = prevLog.Index + len(args.Entries)
				rf.nextIndex[i] = rf.matchIndex[i] + 1
				rf.updateCommitIndex()
			} else if reply.Term > args.Term {
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.changeStateTo(FOLLOWER)
			} else {
				// 快速回退
				// 根据reply.XTerm和reply.XIndex，找到下一个需要同步的位置
				if reply.XTerm == -1 {
					rf.nextIndex[i] = reply.XIndex
				} else {
					nextIndex := 0
					for index := len(rf.logs) - 1; index >= 1; index-- {
						if rf.logs[index].Term == reply.XTerm {
							nextIndex = index
							break
						}
					}
					rf.nextIndex[i] = max(1, min(rf.nextIndex[i]-1, nextIndex))
				}
			}

		}(i)
	}
}

// 更新commitIndex
// 注意必须带锁rf.mu掉用该函数
func (rf *Raft) updateCommitIndex() {
	for i := len(rf.logs) - 1; i > rf.commitIndex; i-- {
		// important bug
		// commitIndex 只能移动到当前Term的地方，如果本轮没有新log，
		// 无法commit任何东西
		// Reason：如果只提交老Leader地Log...因为老Leader有很多版本嘛，
		// 有term3的A被选成Leader 5 with term 1, 2, 3，如果提交了term3的log，
		// 它crash掉了时候，有term 4的log的B复活了，几轮选举后成了Leader 7 with term 1, 2, 4，
		// 他可以提交term 4。这时候，原来提交的term 3被覆盖掉了！这是不行的，已经提交的log不能变！
		//
		// 总之，老Leader的term多种多样，活着的比较旧，然后死了，新一些地接替他，会把原来提交地覆盖掉。
		// 因此，我们必须要把最新得term一起log下去才行，这样再新的老Leader也比最新的旧，
		// 复活后不可能成为新Leader了， solve the problem。
		if rf.logs[i].Term == rf.currentTerm {
			matchedNum := 0
			for peerIdx := range rf.peers {
				if rf.matchIndex[peerIdx] >= i {
					matchedNum++
				}
			}
			if matchedNum > len(rf.peers)/2 {
				rf.commitIndex = i
				// 应用到状态机等后续操作
				break
			}
		}
	}
}

func (rf *Raft) heartBeatHandler() {
	for !rf.killed() {
		rf.lockState()
		if rf.state != LEADER {
			rf.unlockState()
			continue
		}
		rf.unlockState()

		rf.heartBeat()

		time.Sleep(rf.heartBeatDuration)
	}
}
