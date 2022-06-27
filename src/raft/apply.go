package raft

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// a new goroutine to run it
// need acquire lock
func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		entry, err := rf.getEntry(i)
		if err < 0 {
			Debug(dCommit, "S%d apply %v fail(transfer out of range)", rf.me, i)
			continue
		}
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Cmd,
			CommandIndex: entry.Index,
		}
		rf.lastApplied = i
		Debug(dCommit, "S%d apply %v", rf.me, rf.lastApplied)
	}
}
