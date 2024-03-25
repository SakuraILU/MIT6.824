package raft

func (rf *Raft) applier() {
	for !rf.killed() {
		// time.Sleep(10 * time.Millisecond)

		rf.lockState()
		for rf.lastApplied >= rf.logs.getLastIndex() || rf.lastApplied >= rf.commitIndex {
			// important bug
			// 在提交不了了的时候才Sleep一会儿！
			// 放在for开头的话，每次扔一个applyMsg都会等待
			// 上层应用拿applied logs很慢！有些极端的测试用例过不了，
			// 可能它还没提交完logs就死了
			rf.applyCond.Wait()
		}

		// rf.lastApplied++
		rf.setLastAppliedSafe(rf.lastApplied + 1)
		log := rf.logs.getLog(rf.lastApplied)
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      log.Command,
			CommandTerm:  log.Term,
			CommandIndex: log.Index,
		}
		DPrintf("[Raf/t %d {%d}] apply log: %v", rf.me, rf.currentTerm, log)
		rf.unlockState()

		rf.applyCh <- applyMsg
	}
}
