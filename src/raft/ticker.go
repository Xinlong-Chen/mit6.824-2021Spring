package raft

import (
	"time"
	"math/rand"
)

const (
	gap_time            int = 3
	election_base_time  int = 300
	election_range_time int = 100
	heartbeat_time      int = 50
)

func (rf *Raft) electionTimeout() bool {
	return time.Now().After(rf.electionTime)
}

func (rf *Raft) heartbeatTimeout() bool {
	return time.Now().After(rf.heartbeatTime)
}

func (rf *Raft) resetElectionTime() {
	sleep_time := rand.Intn(election_range_time) + election_base_time
	rf.electionTime = time.Now().Add(time.Millisecond * time.Duration(sleep_time))
}

func (rf *Raft) resetHeartbeatTime() {
	rf.heartbeatTime = time.Now().Add(time.Millisecond * time.Duration(heartbeat_time))
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
				Debug(dTimer, "S%d Election timeout, Start election, T%d", rf.me, rf.currentTerm)
				rf.TurnTo(candidate)
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
				rf.resetHeartbeatTime()
				rf.doAppendEntries(false)
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(gap_time) * time.Millisecond)
	}
}