package raft

import (
	"bytes"
	"log"

	"mit6.824/src/labgob"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	//time | short file name | line number
	log.SetFlags(log.Lmicroseconds)
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) lockState() {
	rf.mu.Lock()
}

func (rf *Raft) unlockState() {
	// a little tricky
	// 注意状态更改都是带rf.mu锁的，
	// 因此unlock也就意味着修改rf状态的操作结束，可以持久化了
	rf.persist()
	rf.mu.Unlock()
}

// 必须带锁rf.mu调用该函数
func (rf *Raft) stateUnchanged(state State, term int) bool {
	return rf.state == state && rf.currentTerm == term
}

func (rf *Raft) statePersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	return w.Bytes()
}

// 单增地更新currentTerm
// 避免过时的AE RPC等
func (rf *Raft) setCommitIndexSafe(index int) {
	prevCommitIndex := rf.commitIndex
	rf.commitIndex = max(rf.commitIndex, index)
	// 有新的commitIndex，通知applyer向应用线程提交新的日志
	if rf.commitIndex > prevCommitIndex {
		rf.applyCond.Broadcast()
	}
}

// 单增地更新lastApplied
func (rf *Raft) setLastAppliedSafe(index int) {
	if rf.lastApplied > rf.commitIndex {
		panic("lastApplied > commitIndex")
	}
	if index >= rf.logs.getLogLength() {
		panic("index >= logs length")
	}
	rf.lastApplied = max(rf.lastApplied, index)
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
