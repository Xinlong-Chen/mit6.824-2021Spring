package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) electionTimeout() bool {
	return time.Now().After(rf.electionTime)
}

func (rf *Raft) heartbeatTimeout() bool {
	return time.Now().After(rf.heartbeatTime)
}

func (rf *Raft) resetElectionTime() {
	sleep_time := rand.Intn(election_range_time) + election_base_time
	rf.electionTime = time.Now().Add(time.Duration(sleep_time) * time.Millisecond)
}

func (rf *Raft) resetHeartbeatTime() {
	rf.heartbeatTime = time.Now().Add(time.Duration(heartbeat_time) * time.Millisecond)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		switch rf.status {
		case follower:
			if rf.electionTimeout() {
				rf.TurnTo(candidate)
				Debug(dTimer, "S%d Election timeout, Start election, T%d", rf.me, rf.currentTerm)
				rf.doElection()
				rf.resetElectionTime()
			}
		case candidate:
			if rf.electionTimeout() {
				rf.TurnTo(candidate)
				Debug(dTimer, "S%d Election timeout, re-start election, T%d", rf.me, rf.currentTerm)
				rf.doElection()
				rf.resetElectionTime()
			}
		case leader:
			if rf.heartbeatTimeout() {
				Debug(dTimer, "S%d Heartbeat timeout, send heartbeat boardcast, T%d", rf.me, rf.currentTerm)
				rf.doAppendEntries()
				rf.resetHeartbeatTime()
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(gap_time) * time.Millisecond)
	}
}
