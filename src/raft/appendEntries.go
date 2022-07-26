package raft

import "6.824/utils"

// ticker() call doAppendEntries(), ticker() hold lock
// if a node turn to leader, leader will call doAppendEntries() to send a heartbeat
func (rf *Raft) doAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		wantSendIndex := rf.nextIndex[i] - 1
		if wantSendIndex < rf.frontLogIndex() {
			go rf.doInstallSnapshot(i)
		} else {
			go rf.appendTo(i)
		}
	}
}

func (rf *Raft) appendTo(peer int) {
	rf.mu.Lock()
	if rf.status != leader {
		utils.Debug(utils.DWarn, "S%d status change, it is not leader", rf.me)
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

	// utils.Debug(utils.DTrace, "S%d log length: %d, nextIndex:{%+v}", rf.me, len(rf.log), rf.nextIndex)
	// 0 <= prevLogIndex <= len(log) - 1
	prevLogIndex := rf.nextIndex[peer] - 1
	idx, err := rf.transfer(prevLogIndex)
	if err < 0 {
		rf.mu.Unlock()
		return
	}

	args.PrevLogIndex = rf.log[idx].Index
	args.PrevLogTerm = rf.log[idx].Term

	// must copy in here
	entries := rf.log[idx+1:]
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
		utils.Debug(utils.DInfo, "S%d old response from C%d, ignore it", rf.me, peer)
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		utils.Debug(utils.DTerm, "S%d S%d term larger(%d > %d)", rf.me, peer, reply.Term, rf.currentTerm)
		rf.currentTerm, rf.votedFor = reply.Term, voted_nil
		rf.persist()
		rf.TurnTo(follower)
		return
	}

	if reply.Success {
		// utils.Debug(utils.DTrace, "S%d before nextIndex:{%+v} ", rf.me, rf.nextIndex)
		rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
		// utils.Debug(utils.DTrace, "S%d after nextIndex:{%+v}", rf.me, rf.nextIndex)
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.toCommit()
		return
	}

	if reply.XTerm == -1 { // null slot
		rf.nextIndex[peer] -= reply.XLen
	} else if reply.XTerm >= 0 {
		termNotExit := true
		for index := rf.nextIndex[peer] - 1; index >= 1; index-- {
			entry, err := rf.getEntry(index)
			if err < 0 {
				continue
			}

			if entry.Term > reply.XTerm {
				continue
			}

			if entry.Term == reply.XTerm {
				rf.nextIndex[peer] = index + 1
				termNotExit = false
				break
			}
			if entry.Term < reply.XTerm {
				break
			}
		}
		if termNotExit {
			rf.nextIndex[peer] = reply.XIndex
		}
	} else {
		rf.nextIndex[peer] = reply.XIndex
	}

	// utils.Debug(utils.DTrace, "S%d nextIndex:{%+v}", rf.me, rf.nextIndex)
	// the smallest nextIndex is 1
	// otherwise, it will cause out of range error
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
}
