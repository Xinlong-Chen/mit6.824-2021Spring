package raft

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != leader {
		Debug(dClient, "S%d Not leader cmd: %+v", rf.me, command)
		return -1, -1, false
	}

	index := rf.lastLogIndex() + 1
	rf.log = append(rf.log, Entry{index, rf.currentTerm, command})
	rf.persist()

	defer Debug(dLog2, "S%d append log: %+v", rf.me, rf.log)
	Debug(dClient, "S%d cmd: %+v, logIndex: %d", rf.me, command, rf.lastLogIndex())

	return rf.lastLogIndex(), rf.currentTerm, true
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// refuse to install a snapshot
	if rf.frontLogIndex() >= index {
		Debug(dSnap, "S%d have received %d snapshot", rf.me, index)
		return
	}

	idx, err := rf.transfer(index)
	if err < 0 {
		idx = len(rf.log) - 1
	}

	// let last snapshot node as dummy node
	rf.log = rf.log[idx:]
	rf.log[0].Cmd = nil // dummy node
	rf.persistSnapshot(snapshot)
}
