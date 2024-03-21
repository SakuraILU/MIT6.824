package raft

import (
	"log"
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
	rf.persist()
	rf.mu.Unlock()
}

// 必须带锁rf.mu调用该函数
func (rf *Raft) stateUnchanged(state State, term int) bool {
	return rf.state == state && rf.currentTerm == term
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
