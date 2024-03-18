package raft

import "time"

func (rf *Raft) applier() {
	for !rf.killed() {
		// time.Sleep(10 * time.Millisecond)

		rf.lockState()
		if rf.lastApplied >= rf.commitIndex {
			rf.unlockState()
			// important bug
			// 在提交不了了的时候才Sleep一会儿！
			// 放在for开头的话，每次扔一个applyMsg都会等待
			// 上层应用拿applied logs很慢！有些极端的测试用例过不了，
			// 可能它还没提交完logs就死了
			time.Sleep(10 * time.Millisecond)
			continue
		}

		rf.lastApplied++
		log := rf.logs[rf.lastApplied]
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      log.Command,
			CommandIndex: log.Index,
		}
		DPrintf("[Raft %d {%d}] apply log: %v", rf.me, rf.currentTerm, log)
		rf.unlockState()

		rf.applyCh <- applyMsg
	}
}
