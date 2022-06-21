package raft

func (rf *Raft) doAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := AppendEntriesReply{}
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.LeaderCommit = rf.commitIndex
			// must copy in here
			entries := rf.log[rf.nextIndex[i]:]
			args.Entries = make([]Entry, len(entries))
			copy(args.Entries, entries)
			rf.mu.Unlock()

			Debug(dTrace, "S%d send request {%+v}", rf.me, args)
			ok := rf.sendAppendEntries(i, &args, &reply)
			if !ok {
				Debug(dWarn, "S%d call (AppendEntries)rpc to C%d error", rf.me, i)
				return
			}
			Debug(dTrace, "S%d get response {%+v}", rf.me, reply)

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

			if reply.Success {
				rf.nextIndex[i] += len(args.Entries)
				rf.matchIndex[i] = rf.nextIndex[i] - 1
				Debug(dTrace, "S%d nextindex {%+v}, match {%+v}", rf.me, rf.nextIndex, rf.matchIndex)
				rf.toCommit()
			} else {
			}
		}(i)
	}
}
