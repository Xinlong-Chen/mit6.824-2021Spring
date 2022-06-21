package raft

import (
	"math/rand"
	"time"
)

type ServerStatus string

const (
	follower  ServerStatus = "Follower"
	candidate ServerStatus = "Candidate"
	leader    ServerStatus = "Leader"
)

const (
	// magic number
	voted_nil int = -10086
)

const (
	base_time     int = 250
	range_time    int = 100
	heart_timeout int = 50
)

// without lock
// if have a new goroutine, must lock it !!!
func (rf *Raft) TurnTo(status ServerStatus) {
	defer Debug(dTrace, "S%d converting to %v in T(%d)", rf.me, rf.status, rf.currentTerm)
	switch status {
	case follower:
		rf.status = follower
	case candidate:
		// • Increment currentTerm
		rf.currentTerm++
		// • Vote for self
		rf.votedFor = rf.me
		rf.status = candidate
	case leader:
		rf.status = leader
		rf.leaderInit()
		// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
		// repeat during idle periods to prevent election timeouts (§5.2)
		rf.doAppendEntries()
	}
}

func (rf *Raft) election_timeout() time.Duration {
	sleep_time := rand.Intn(range_time) + base_time
	return time.Duration(sleep_time) * time.Millisecond
}

func (rf *Raft) heart_timeout() time.Duration {
	return time.Duration(heart_timeout) * time.Millisecond
}