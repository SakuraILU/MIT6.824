package raft

import "fmt"

type State int

func (s State) String() string {
	switch s {
	case FOLLOWER:
		return "follower"
	case CANDIDATE:
		return "candidate"
	case LEADER:
		return "leader"
	}

	return "unknown"
}

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

// 必须带锁rf.mu掉用该函数
func (rf *Raft) changeStateTo(state State) {
	switch rf.state {
	case FOLLOWER:
		{
			if state == FOLLOWER {
				rf.voteFor = -1
				// rf.resetElectionTimer()
			} else if state == CANDIDATE {
				rf.currentTerm++
				rf.voteFor = rf.me
				// rf.resetElectionTimer()
			} else {
				panic(fmt.Sprintf("Invalid state change from %v to %v", rf.state, state))
			}
			break
		}
	case CANDIDATE:
		{
			if state == FOLLOWER {
				rf.voteFor = -1
				// rf.resetElectionTimer()
			} else if state == CANDIDATE {
				rf.currentTerm++
				rf.voteFor = rf.me
				// rf.resetElectionTimer()
			} else if state == LEADER {
				// 注意Leader才需要维护nextIndex和matchIndex，用与更新commitIndex
				// 而follower的commitIndex是Leader同步过去的
				for peerIdx := range rf.peers {
					rf.nextIndex[peerIdx] = rf.logs.getLogLength()
					rf.matchIndex[peerIdx] = -1
				}
				rf.matchIndex[rf.me] = rf.logs.getLogLength() - 1
			} else {
				panic(fmt.Sprintf("Invalid state change from %v to %v", rf.state, state))
			}
			break
		}
	case LEADER:
		{
			if state == FOLLOWER {
				rf.voteFor = -1
				// rf.resetElectionTimer()
			} else {
				panic(fmt.Sprintf("Invalid state change from %v to %v", rf.state, state))
			}
			break
		}
	}
	rf.state = state

	rf.stateCond.Broadcast()
}
