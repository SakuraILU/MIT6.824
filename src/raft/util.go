package raft

import (
	"log"
)

// Debugging
const Debug = 1

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
