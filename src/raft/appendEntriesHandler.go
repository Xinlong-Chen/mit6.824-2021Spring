package raft

import "6.824/utils"

// handler need to require lock
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	utils.Debug(utils.DLog, "S%d S%d appendEntries", rf.me, args.LeaderId)
	defer utils.Debug(utils.DLog, "S%d arg: %+v reply: %+v", rf.me, args, reply)

	defer rf.persist()

	if args.Term < rf.currentTerm { // leader out, refuse
		reply.Term = rf.currentTerm
		reply.Success = false
		utils.Debug(utils.DTerm, "S%d S%d term less(%d < %d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		rf.currentTerm, rf.votedFor = args.Term, voted_nil
		utils.Debug(utils.DTerm, "S%d S%d term larger(%d > %d)", rf.me, args.LeaderId, args.Term, rf.currentTerm)
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

	if args.PrevLogIndex < rf.frontLogIndex() {
		reply.XTerm, reply.XIndex, reply.Success = -2, rf.frontLogIndex() + 1, false
		utils.Debug(utils.DInfo, "S%d args's prevLogIndex too smaller(%v < %v)", rf.me, args.PrevLogIndex, rf.frontLogIndex())
		return
	}

	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - rf.lastLogIndex()
		return
	}

	idx, err := rf.transfer(args.PrevLogIndex)
	if err < 0 {
		return
	}

	if rf.log[idx].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.log[idx].Term
		reply.XIndex = args.PrevLogIndex
		// 0 is a dummy entry => quit in index is 1
		// binary search is better than this way
		for index := idx; index >= 1; index-- {
			if rf.log[index-1].Term != reply.XTerm {
				reply.XIndex = index
				break
			}
		}
		return
	}

	if args.Entries != nil && len(args.Entries) != 0 {
		if rf.isConflict(args) {
			rf.log = rf.log[:idx+1]
			entries := make([]Entry, len(args.Entries))
			copy(entries, args.Entries)
			rf.log = append(rf.log, entries...)
			// utils.Debug(utils.DInfo, "S%d conflict, truncate log: %+v", rf.me, rf.log)
		} else {
			// utils.Debug(utils.DInfo, "S%d no conflict, log: %+v", rf.me, rf.log)
		}
	} else {
		utils.Debug(utils.DInfo, "S%d args entries nil or length is 0: %v", rf.me, args.Entries)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > rf.lastLogIndex() {
			rf.commitIndex = rf.lastLogIndex()
		}
		utils.Debug(utils.DCommit, "S%d commit to %v(lastLogIndex: %d)", rf.me, rf.commitIndex, rf.lastLogIndex())
		rf.applyCond.Signal()
	}
	// utils.Debug(utils.DInfo, "S%d log: %+v", rf.me, rf.log)
}

func (rf *Raft) isConflict(args *AppendEntriesArgs) bool {
	base_index := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		entry_rf, err := rf.getEntry(i + base_index)
		if err < 0 {
			return true
		}
		if entry_rf.Term != entry.Term {
			return true
		}
	}
	return false
}
