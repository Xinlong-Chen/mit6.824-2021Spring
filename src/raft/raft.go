package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/utils"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	applyCond *sync.Cond          // haven't used now, it seem can be used for apply
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	status    ServerStatus
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent for all servers
	currentTerm int
	votedFor    int
	log         []Entry

	// volatile for all servers
	commitIndex int
	lastApplied int

	// volatile for leaders
	nextIndex  []int
	matchIndex []int

	// private
	electionTime  time.Time
	heartbeatTime time.Time
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) leaderInit() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}

	rf.resetHeartbeatTime()
}

func (rf *Raft) init() {
	rf.status = follower
	rf.applyCond = sync.NewCond(&rf.mu)
	// persistent for all servers
	rf.currentTerm = 0
	rf.votedFor = voted_nil // means that vote for nobody
	rf.log = make([]Entry, 0)
	// use first log entry as last snapshot index
	// also it's dummy node!!
	rf.log = append(rf.log, Entry{magic_index, magic_term, nil})
	// volatile for all servers, will be changed in persister read
	rf.commitIndex = 0
	rf.lastApplied = 0
	// private
	// begin with follower, set election time
	rf.resetElectionTime()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		applyCh:   applyCh,
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.init()

	utils.Debug(utils.DClient, "S%d Started && init success", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLog()

	return rf
}
