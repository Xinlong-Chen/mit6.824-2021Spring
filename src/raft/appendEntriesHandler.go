package raft

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dLog, "S%d S%d appendEntries", rf.me, args.LeaderId)

	if args.Term < rf.currentTerm { // leader out, refuse
		reply.Term = rf.currentTerm
		reply.Success = false
		Debug(dTerm, "S%d S%d term less(%d < %d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	} else if args.Term > rf.currentTerm { // turn to follower
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		rf.currentTerm, rf.votedFor = args.Term, voted_nil
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
	rf.electionTimer.Reset(rf.election_timeout())

	// heartbeat, return
	if args.PrevLogIndex == magic_index && args.PrevLogTerm == magic_term {
		return
	}

	// attention: must delete overdue data first
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - rf.lastLogIndex()
		Debug(dTrace, "S%d arg: %+v reply: %+v {log: %+v}", rf.me, args, reply, rf.log)
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
		Debug(dTrace, "S%d arg: %+v reply: %+v {log: %+v}", rf.me, args, reply, rf.log)
		return
	}

	Debug(dLog2, "S%d before: log: %+v", rf.me, rf.log)
	origin_end, add_begin := args.PrevLogIndex+1, 0
	for ; origin_end <= rf.lastLogIndex() && add_begin < len(args.Entries); origin_end, add_begin = origin_end+1, add_begin+1 {
		if rf.log[origin_end].Term != args.Entries[add_begin].Term {
			break
		}
	}
	rf.log = rf.log[:origin_end]
	rf.log = append(rf.log, args.Entries[add_begin:]...)
	Debug(dLog2, "S%d after append: log: %+v", rf.me, rf.log)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > rf.lastLogIndex() {
			rf.commitIndex = rf.lastLogIndex()
		}
		Debug(dCommit, "S%d commit to %v(lastLogIndex: %d)", rf.me, rf.commitIndex, rf.lastLogIndex())
		go rf.applyLog()
	}
}
