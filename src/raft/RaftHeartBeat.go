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
	if args.PrevLogIndex >= rf.logs.getLogLength() {
		reply.Term = rf.currentTerm
		reply.Success = false

		reply.XTerm = -1
		reply.XIndex = rf.logs.getLogLength()
		return
	}
	// 对比同一位置的Log是否与Leader的PrevLog相同
	prevLog := rf.logs.getLog(args.PrevLogIndex)
	if prevLog.Term != args.PrevLogTerm || prevLog.Index != args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		// 寻找与冲突的PrevLogTerm相同Term的第一个Index，
		// 让Leader发送其之后的所有元素
		reply.XTerm = prevLog.Term
		reply.XIndex = rf.logs.getLastIncludedIndex() + 1
		for i := prevLog.Index; i > rf.logs.getLastIncludedIndex(); i-- {
			if rf.logs.getLog(i-1).Term != prevLog.Term {
				reply.XIndex = i
				break
			}
		}
		return
	}
	// 从PrevLogIndex+1同步Leader的日志
	rf.logs.appendLogs(args.PrevLogIndex+1, args.Entries)
	DPrintf("[Raft %d {%d}] AppendEntries success, log length: %d", rf.me, rf.currentTerm, rf.logs.getLogLength())
	// 同步Leader的commitIndex
	rf.commitIndex = min(args.LeaderCommit, rf.logs.getLastIndex())

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

			if rf.nextIndex[i] <= rf.logs.getLastIncludedIndex() {
				// 如果nextIndex[i] 在snapshot里面，发送InstallSnapshot给follower，
				// 先把snapshot同步过去
				args := &InstallSnapshotArgs{
					LeaderId:          rf.me,
					Term:              rf.currentTerm,
					LastIncludedIndex: rf.logs.getLastIncludedIndex(),
					LastIncludedTerm:  rf.logs.getLastIncludedTerm(),
					Data:              rf.snapshot,
				}
				rf.unlockState()

				reply := &InstallSnapshotReply{}
				DPrintf("[Raft %d {%d}] sendInstallSnapshot to %d", rf.me, rf.currentTerm, i)
				ok := rf.sendInstallSnapshot(i, args, reply)
				if !ok {
					return
				}

				rf.lockState()
				defer rf.unlockState()
				// important bug
				// 过时的InstallSnapshot RPC，不用理会
				if !rf.stateUnchanged(LEADER, args.Term) {
					return
				}

				if reply.Term > args.Term {
					// 如果Term更大，变成FOLLOWER
					rf.currentTerm = reply.Term
					rf.voteFor = -1
					rf.changeStateTo(FOLLOWER)
				} else {
					// 如果snapshot同步成功，更新nextIndex和matchIndex
					rf.nextIndex[i] = rf.logs.getLastIncludedIndex() + 1
					rf.matchIndex[i] = rf.logs.getLastIncludedIndex()
				}
			} else {
				// 如果nextIndex[i] 在logs里面，发送AppendEntries给follower，
				// 同步后续的log
				entries := make([]*LogEntry, 0)
				if rf.nextIndex[i] < rf.logs.getLogLength() {
					entries = append(entries, rf.logs.getLogsFrom(rf.nextIndex[i])...)
				}
				prevLog := rf.logs.getLog(rf.nextIndex[i] - 1)
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
				DPrintf("[Raft %d {%d}] sendAppendEntries to %d", rf.me, rf.currentTerm, i)
				ok := rf.sendAppendEntries(i, args, reply)
				if !ok {
					return
				}

				rf.lockState()
				defer rf.unlockState()
				// important bug
				// 过时的AppendEntries RPC，不用理会
				if !rf.stateUnchanged(LEADER, args.Term) {
					return
				}

				if reply.Success {
					// 如果同步成功，更新nextIndex和matchIndex
					rf.matchIndex[i] = prevLog.Index + len(args.Entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					// matchIndex[i]更新，可能导致commitIndex更新
					rf.updateCommitIndex()
				} else if reply.Term > args.Term {
					// 如果Term更大，变成FOLLOWER
					rf.currentTerm = reply.Term
					rf.voteFor = -1
					rf.changeStateTo(FOLLOWER)
				} else {
					// 失败时，根据reply的信息做快速回退
					if reply.XTerm == -1 {
						// 如果没有冲突的Term，follower的logs太短了，
						// reply.XIndex指明了follower logs的末尾，同步后面的logs
						rf.nextIndex[i] = reply.XIndex
					} else {
						// 如果有冲突的Term，冲突的位置应该在这个XTerm的某个记录上，
						// reply.XIndex是冲突Term的第一个Index，这是悲观估计，需要同步进可能多。
						// 悲观估计有时会导致很多多余的logs同步，尤其是在同一个Term中运行了很久的Leader的记录中发生了冲突时。
						// 因此这里需要找到XTerm的最后一个Index，乐观估计。如果冲突位置在乐观估计之前，就保守地一格一格往前走。
						nextIndex := rf.logs.getLastIncludedIndex()
						for index := rf.logs.getLogLength() - 1; index > rf.logs.getLastIncludedIndex(); index-- {
							if rf.logs.getLog(index).Term == reply.XTerm {
								nextIndex = index
								break
							}
						}
						// a little trick，乐观估计快速回退和保守回退法取最小，其实就是上述效果：先乐观快回再保守回退
						rf.nextIndex[i] = min(rf.nextIndex[i]-1, nextIndex)
					}
				}
			}
		}(i)
	}
}

// 更新commitIndex
// 注意必须带锁rf.mu掉用该函数
func (rf *Raft) updateCommitIndex() {
	for i := rf.logs.getLogLength() - 1; i > rf.commitIndex; i-- {
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
		if rf.logs.getLog(i).Term == rf.currentTerm {
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
