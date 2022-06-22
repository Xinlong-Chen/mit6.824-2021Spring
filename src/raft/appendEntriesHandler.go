package raft

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dLog, "S%d S%d appendEntries", rf.me, args.LeaderId)

	if args.Term < rf.currentTerm { // leader out, refuse
		reply.Term = rf.currentTerm
		reply.Success = false
		Debug(dLog, "S%d S%d term less(%d < %d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
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

	// TODO
	// implement log append

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > rf.lastLogIndex() {
			rf.commitIndex = rf.lastLogIndex()
		}
		Debug(dCommit, "S%d commit to %v(lastLogIndex: %d)", rf.me, rf.commitIndex, rf.lastLogIndex())
		go rf.applyLog()
	}

	if args.PrevLogIndex > rf.lastLogIndex() || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	base_index := args.PrevLogIndex + 1
	ago_len := len(rf.log)
	for i, entry := range args.Entries {
		// if rf.log[base_index + i].term !=
		if base_index+i < ago_len {
			if rf.log[base_index+i].Term != entry.Term {
				rf.log[base_index+i] = entry
			} else {
				continue
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}
	Debug(dCommit, "S%d log len %v", rf.me, len(rf.log))
}
