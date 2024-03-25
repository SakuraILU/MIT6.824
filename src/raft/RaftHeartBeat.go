package raft

import (
	"fmt"
	"time"
)

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

var heartBeatDuration = 40 * time.Millisecond

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

	// 如果PrevLogIndex在snapshot里面，snapshot部分已经提交，不需要再同步
	// 直接同步后续全量日志，用XTerm = -1作为精准回退的标志
	if args.PrevLogIndex < rf.logs.getLastIncludedIndex() {
		reply.Term = rf.currentTerm
		reply.Success = false

		reply.XTerm = -1
		reply.XIndex = rf.logs.getLastIncludedIndex() + 1
		return
	}

	// 如果PrevLogIndex在logs里面，检查PrevLogTerm是否匹配
	prevLog := rf.logs.getLog(args.PrevLogIndex)
	if prevLog.Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false

		reply.XTerm = prevLog.Term
		// 找到一个悲观估计的冲突位置
		// 从PrevLogIndex-1开始往前找，找到冲突Term（prevLogTerm）的第一个位置
		// 虽然这个位置不一定是真正的冲突位置，但是可以保证这个位置之后的log都是冲突的
		// 然而。。。Leader其实没有用上，Leader用的是同Term的悲观估计，悲观估计失败后
		// 继续往前保守回退，所以这个悲观估计的冲突位置是没用的，只是为了和Paper一致
		reply.XIndex = rf.logs.getLastIncludedIndex() + 1
		for i := args.PrevLogIndex - 1; i >= rf.logs.getLastIncludedIndex(); i-- {
			if rf.logs.getLog(i).Term != prevLog.Term {
				reply.XIndex = i + 1
				break
			}
		}
		return
	}

	// 从PrevLogIndex+1同步Leader的日志
	rf.logs.appendLogs(args.PrevLogIndex+1, args.Entries)
	DPrintf("[Raft %d {%d}] AppendEntries success, log length: %d", rf.me, rf.currentTerm, rf.logs.getLogLength())
	// 同步Leader的commitIndex
	// important bug
	// 切换Leader后，新Leader的commitIndex可能比老Leader的commitIndex小
	// 而follower本身也是比老Leader小的，不太好论证新Leader的commitIndex在切换后一定比follower大
	// 为了使得commitIndex不会回退，这里需要取Leader和follower的commitIndex的最大值
	// where to locate this bug：updateCommitIndex(): invalid getLog(i)
	// rf.commitIndex = max(args.LeaderCommit, rf.commitIndex)
	rf.setCommitIndexSafe(args.LeaderCommit)
	if rf.commitIndex >= rf.logs.getLogLength() {
		panic(fmt.Sprintf("commitIndex: %d, log length: %d", rf.commitIndex, rf.logs.getLogLength()))
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ch := make(chan bool, 1) // 创建一个缓冲为1的channel

	// 启动一个goroutine来执行RPC调用
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		ch <- ok // 将RPC调用的结果发送到channel中
	}()

	select {
	case ok := <-ch:
		// 成功接收到RPC调用的结果
		return ok
	case <-time.After(rf.sendRPCTimeout):
		// 超时
		return false
	}
}

// 向某一个peer发送心跳的线程
func (rf *Raft) heartBeatForPeer(i int) {
	for !rf.killed() {
		rf.lockState()

		rf.heartBeatConds[i].Wait()
		for rf.state != LEADER {
			rf.heartBeatConds[i].Wait()
		}

		if rf.nextIndex[i] <= rf.logs.getLastIncludedIndex() {
			// 如果nextIndex[i] 在snapshot说面，之前的log已经被snapshot了，
			// 所以直接发送InstallSnapshot给follower，先把snapshot同步过去
			// 之后再继续同步后续logs
			args := &InstallSnapshotArgs{
				LeaderId:          rf.me,
				Term:              rf.currentTerm,
				LastIncludedIndex: rf.logs.getLastIncludedIndex(),
				LastIncludedTerm:  rf.logs.getLastIncludedTerm(),
				Data:              rf.persister.ReadSnapshot(),
			}
			rf.unlockState()

			reply := &InstallSnapshotReply{}
			DPrintf("[Raft %d {%d}] sendInstallSnapshot to %d", rf.me, rf.currentTerm, i)
			ok := rf.sendInstallSnapshot(i, args, reply)
			if !ok {
				continue
			}

			rf.lockState()
			// important bug
			// 过时的InstallSnapshot RPC，不用理会
			if !rf.stateUnchanged(LEADER, args.Term) {
				rf.unlockState()
				continue
			}

			if reply.Term > args.Term {
				// 如果Term更大，变成FOLLOWER
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.changeStateTo(FOLLOWER)
			} else {
				// 如果snapshot同步成功，更新nextIndex和matchIndex
				// 注意RPC中途logs可能会因为调用了Start()等发生变化，所以需要用发送RPC时的args.LastIncludedIndex，
				// 而不是直接用rf.logs.getLogLength()之类的，以保证调用前后的一致性
				rf.nextIndex[i] = args.LastIncludedIndex + 1
				rf.matchIndex[i] = args.LastIncludedIndex
				rf.updateCommitIndex()
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
				continue
			}

			rf.lockState()
			// important bug
			// 过时的AppendEntries RPC，不用理会
			if !rf.stateUnchanged(LEADER, args.Term) {
				rf.unlockState()
				continue
			}

			if reply.Success {
				// 如果同步成功，更新nextIndex和matchIndex
				// 注意RPC中途logs可能会因为调用了Start()等发生变化，所以需要用len(args.Entries)，
				// 而不是直接用rf.logs.getLogLength()之类的，以保证调用前后的一致性
				rf.nextIndex[i] = prevLog.Index + len(args.Entries) + 1
				rf.matchIndex[i] = rf.nextIndex[i] - 1
				// matchIndex[i]更新，可能导致commitIndex更新
				rf.updateCommitIndex()
			} else if reply.Term > args.Term {
				// 如果Term更大，变成FOLLOWER
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.changeStateTo(FOLLOWER)
			} else {
				// 简化，简化，简化。。。直接回退到Follower指出的悲观估计位置
				// 悲观估计位置是ConflictTerm的第一个位置，下一次传的PrevLogTerm必然更小，
				// 然后Follower就可以在更前一个Term中定位悲观估计位置，work！
				rf.nextIndex[i] = reply.XIndex
			}
		}

		rf.unlockState()
	}
}

// 必须带锁rf.mu访问，非阻塞
func (rf *Raft) heartBeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// 通知所有心跳线程
		rf.heartBeatConds[i].Signal()
	}

}

func (rf *Raft) heartBeatHandler() {
	// 启动心跳线程
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.heartBeatForPeer(i)
	}

	for !rf.killed() {
		rf.lockState()
		for rf.state != LEADER {
			rf.stateCond.Wait()
		}

		rf.heartBeat()

		rf.unlockState()

		time.Sleep(rf.heartBeatDuration)
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
				// rf.commitIndex = i
				rf.setCommitIndexSafe(i)
				// 应用到状态机等后续操作
				break
			}
		}
	}
}
