package raft

type InstallSnapshotArgs struct {
	LeaderId          int    // 领导者的ID
	Term              int    // 领导者的Term
	LastIncludedIndex int    // snapshot中压缩的最后一个LogIndex
	LastIncludedTerm  int    // snapshot中压缩的最后一个LogTerm
	Data              []byte // snapshot中的一块Data
}

type InstallSnapshotReply struct {
	Term int // 跟随者的Term
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lockState()

	// 如果Term小于当前Term，拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.unlockState()
		return
	}
	// 合法的Leader，重置选举定时器
	rf.resetElectionTimer()
	// 如果Term大于当前Term，更新Term并变成FOLLOWER
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeStateTo(FOLLOWER)
	}

	DPrintf("[Raft %d {%d}] InstallSnapshot from %d", rf.me, rf.currentTerm, args.LeaderId)
	reply.Term = rf.currentTerm

	// 如果snapshot中的最后一个LogIndex比当前的小，拒绝，
	// 这是过时的InstallSnapshot RPC
	// 先发的InstallSnapshot RPC可能会在网络中滞留很久
	if args.LastIncludedIndex <= rf.logs.getLastIncludedIndex() {
		rf.unlockState()
		return
	}

	// 尝试在logs中找到snapshot中的最后一个log的位置
	lastIncludedIndex := -1
	for i := rf.logs.getLastIncludedIndex() + 1; i < rf.logs.getLogLength(); i++ {
		log := rf.logs.getLog(i)
		if args.LastIncludedIndex == log.Index && args.LastIncludedTerm == log.Term {
			lastIncludedIndex = i
			break
		}
	}

	if lastIncludedIndex != -1 {
		// 如果找到了，截断之前的Log
		DPrintf("[Raft %d {%d}] InstallSnapshot from %d, lastIncludedIndex: %d", rf.me, rf.currentTerm, args.LeaderId, lastIncludedIndex)
		rf.logs.truncLogBefore(lastIncludedIndex)
	} else {
		// 如果没找到，直接清空logs，从lastIncludedIndex开始
		DPrintf("[Raft %d {%d}] InstallSnapshot from %d, lastIncludedIndex: %d", rf.me, rf.currentTerm, args.LeaderId, args.LastIncludedIndex)
		rf.logs = MakeLogs(args.LastIncludedIndex, args.LastIncludedTerm)
	}
	// 更新snapshot
	rf.snapshot = args.Data

	// 更新并提交Snapshot，相当于之前的logs都被提交了，
	// commitIndex和lastApplied都更新到snapshot的最后一个Index
	rf.commitIndex = rf.logs.getLastIncludedIndex()
	rf.lastApplied = rf.logs.getLastIncludedIndex()

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.logs.getLastIncludedTerm(),
		SnapshotIndex: rf.logs.getLastIncludedIndex(),
	}

	rf.unlockState()

	rf.applyCh <- applyMsg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
