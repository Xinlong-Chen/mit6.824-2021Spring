package raft

// handler need to require lock
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dLog, "S%d S%d appendEntries", rf.me, args.LeaderId)
	defer Debug(dLog, "S%d arg: %+v reply: %+v {log: %+v}", rf.me, args, reply, rf.log)

	defer rf.persist()

	if args.Term < rf.currentTerm { // leader out, refuse
		reply.Term = rf.currentTerm
		reply.Success = false
		Debug(dTerm, "S%d S%d term less(%d < %d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		rf.currentTerm, rf.votedFor = args.Term, voted_nil
		Debug(dTerm, "S%d S%d term larger(%d > %d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		rf.TurnTo(follower)
	}

	if rf.status != follower {
		// If AppendEntries RPC received from new leader:
		// convert to follower
		rf.TurnTo(follower)
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	//  prevent election timeouts (§5.2)
	rf.resetElectionTime()

	// heartbeat, return
	// if args.PrevLogIndex == magic_index && args.PrevLogTerm == magic_term {
	// 	return
	// }

	// attention: must delete overdue data first
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - rf.lastLogIndex()
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		reply.XIndex = args.PrevLogIndex
		// 0 is a dummy entry => quit in index is 1
		// binary search is better than this way
		for index := args.PrevLogIndex; index >= 1; index-- {
			if rf.log[index-1].Term != reply.XTerm {
				reply.XIndex = index
				break
			}
		}
		return
	}

	if args.Entries != nil && len(args.Entries) != 0 {
		if rf.isConflict(args) {
			rf.log = rf.log[:args.PrevLogIndex+1]
			entries := make([]Entry, len(args.Entries))
			copy(entries, args.Entries)
			rf.log = append(rf.log, entries...)
			Debug(dInfo, "S%d conflict, truncate log: %+v", rf.me, rf.log)
		} else {
			Debug(dInfo, "S%d no conflict, log: %+v", rf.me, rf.log)
		}
	} else {
		Debug(dInfo, "S%d args entries nil or length is 0: %v", rf.me, args.Entries)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > rf.lastLogIndex() {
			rf.commitIndex = rf.lastLogIndex()
		}
		Debug(dCommit, "S%d commit to %v(lastLogIndex: %d)", rf.me, rf.commitIndex, rf.lastLogIndex())
		go rf.applyLog()
	}
}

func (rf *Raft) isConflict(args *AppendEntriesArgs) bool {
	base_index := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		if i+base_index > rf.lastLogIndex() {
			return true
		}
		if rf.log[i+base_index].Term != entry.Term {
			return true
		}
	}
	return false
}
