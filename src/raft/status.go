package raft

import (
	"math/rand"
	"time"
)

type ServerStatus int

const (
	follower ServerStatus = iota
	candidate
	leader
)

const (
	base_time     int = 200
	range_time    int = 100
	heart_timeout int = 100
)

// without lock
// if have a new goroutine, must lock it !!!
func (rf *Raft) TurnTo(status ServerStatus) {
	switch status {
	case follower:
		// fmt.Println(rf.me, " will be follower")
		rf.votedFor = voted_nil
		rf.status = follower
	case candidate:
		// fmt.Println(rf.me, " will be candidate")
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.status = candidate
	case leader:
		// fmt.Println(rf.me, " will be leader")
		rf.status = leader
	}
}

func (rf *Raft) election_timeout() time.Duration {
	sleep_time := rand.Intn(range_time) + base_time
	return time.Duration(sleep_time) * time.Millisecond
}

func (rf *Raft) heart_timeout() time.Duration {
	return time.Duration(heart_timeout) * time.Millisecond
}
