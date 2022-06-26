package raft

// ticker() call doAppendEntries(), ticker() hold lock
// if a node turn to leader, leader will call doAppendEntries() to send a heartbeat
func (rf *Raft) doAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.appendTo(i)
	}
}

func (rf *Raft) appendTo(peer int) {
	rf.mu.Lock()
	if rf.status != leader {
		Debug(dWarn, "S%d status change, it is not leader", rf.me)
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: magic_index,
		PrevLogTerm:  magic_term,
		LeaderCommit: rf.commitIndex,
	}

	Debug(dTrace, "S%d log length: %d, nextIndex:{%+v}", rf.me, len(rf.log), rf.nextIndex)

	args.PrevLogIndex = rf.nextIndex[peer] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term

	// must copy in here
	entries := rf.log[rf.nextIndex[peer]:]
	args.Entries = make([]Entry, len(entries))
	copy(args.Entries, entries)
	rf.mu.Unlock()

	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(peer, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// status changed or outdue data, ignore
	if rf.currentTerm != args.Term || rf.status != leader || reply.Term < rf.currentTerm {
		// overdue, ignore
		Debug(dInfo, "S%d old response from C%d, ignore it", rf.me, peer)
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		Debug(dTerm, "S%d S%d term larger(%d > %d)", rf.me, peer, args.Term, rf.currentTerm)
		rf.currentTerm, rf.votedFor = reply.Term, voted_nil
		rf.persist()
		rf.TurnTo(follower)
		return
	}

	if reply.Success {
		// Debug(dTrace, "S%d before nextIndex:{%+v} ", rf.me, rf.nextIndex)
		rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
		// Debug(dTrace, "S%d after nextIndex:{%+v}", rf.me, rf.nextIndex)
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.toCommit()
		return
	}

	if reply.XTerm != -1 {
		termNotExit := true
		for index := rf.nextIndex[peer] - 1; index >= 1; index-- {
			if index > rf.lastLogIndex() || rf.log[index].Term > reply.XTerm {
				continue
			}

			if rf.log[index].Term == reply.XTerm {
				rf.nextIndex[peer] = index + 1
				termNotExit = false
				break
			}
			if rf.log[index].Term < reply.XTerm {
				break
			}
		}
		if termNotExit {
			rf.nextIndex[peer] = reply.XIndex
		}
	} else { // null slot
		rf.nextIndex[peer] -= reply.XLen
	}

	// Debug(dTrace, "S%d nextIndex:{%+v}", rf.me, rf.nextIndex)
	// the smallest nextIndex is 1
	// otherwise, it will cause out of range error
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
}
