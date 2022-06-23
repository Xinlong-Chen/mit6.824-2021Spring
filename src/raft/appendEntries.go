package raft

const (
	magic_index int = -12345
	magic_term  int = -10001
)

func (rf *Raft) doAppendEntries(emptyHeartbeat bool) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.appendTo(emptyHeartbeat, i)
	}
}

func (rf *Raft) appendTo(emptyHeartbeat bool, i int) {
	reply := AppendEntriesReply{}
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: magic_index,
		PrevLogTerm:  magic_term,
	}

	if !emptyHeartbeat {
		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		args.LeaderCommit = rf.commitIndex
		// must copy in here
		entries := rf.log[rf.nextIndex[i]:]
		args.Entries = make([]Entry, len(entries))
		copy(args.Entries, entries)
	}
	rf.mu.Unlock()

	Debug(dTrace, "S%d send request {%+v} to %d", rf.me, args, i)
	ok := rf.sendAppendEntries(i, &args, &reply)
	if !ok {
		Debug(dWarn, "S%d call (AppendEntries)rpc to C%d error", rf.me, i)
		return
	}
	Debug(dTrace, "S%d get response {%+v} from %d", rf.me, reply, i)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != args.Term {
		// overdue, ignore
		Debug(dWarn, "S%d old response from C%d (now:%d req:%d)", rf.me, i, rf.currentTerm, args.Term)
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = reply.Term, voted_nil
		rf.TurnTo(follower)
		return
	}

	// heartbeat, ignore
	if emptyHeartbeat {
		return
	}

	if reply.Success {
		rf.nextIndex[i] += len(args.Entries)
		rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
		Debug(dTrace, "S%d nextindex {%+v}, match {%+v}", rf.me, rf.nextIndex, rf.matchIndex)
		rf.toCommit()
		return
	}

	// TODO: implement it
	if reply.XTerm != -1 {
		termNotExit := true
		for index := rf.lastLogIndex(); index >= 1; index-- {
			if rf.log[index].Term == reply.XTerm {
				rf.nextIndex[i] = index
				termNotExit = false
				Debug(dTrace, "S%d from %d---exit term: nextIndex %+v {log: %+v}", rf.me, i, rf.nextIndex, rf.log)
				break
			} else if rf.log[index].Term < reply.XTerm {
				break
			}
		}
		if termNotExit {
			rf.nextIndex[i] = reply.XIndex
			Debug(dTrace, "S%d from %d+++not exit term: nextIndex %+v {log: %+v}", rf.me, i, rf.nextIndex, rf.log)
		}
	} else { // null slot
		rf.nextIndex[i] -= reply.XLen
		Debug(dTrace, "S%d from %d***null slot: nextIndex %+v {log: %+v}", rf.me, i, rf.nextIndex, rf.log)
	}

	// the smallest nextIndex is 1
	// otherwise, it will cause out of range error
	if rf.nextIndex[i] < 1 {
		rf.nextIndex[i] = 1
	}
}
