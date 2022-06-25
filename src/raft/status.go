package raft


type ServerStatus string

const (
	follower  ServerStatus = "Follower"
	candidate ServerStatus = "Candidate"
	leader    ServerStatus = "Leader"
)

// without lock
// if have a new goroutine, must lock it !!!
func (rf *Raft) TurnTo(status ServerStatus) {
	switch status {
	case follower:
		rf.status = follower
		Debug(dTerm, "S%d converting to %v in T(%d)", rf.me, rf.status, rf.currentTerm)
	case candidate:
		// • Increment currentTerm
		rf.currentTerm++
		// • Vote for self
		rf.votedFor = rf.me
		rf.persist()
		rf.status = candidate
		Debug(dTerm, "S%d converting to %v in T(%d)", rf.me, rf.status, rf.currentTerm)
	case leader:
		rf.status = leader
		rf.leaderInit()
		// print before sending heartbeat
		Debug(dTerm, "S%d converting to %v in T(%d)", rf.me, rf.status, rf.currentTerm)
		// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
		// repeat during idle periods to prevent election timeouts (§5.2)
		rf.doAppendEntries(true)
	}
}

