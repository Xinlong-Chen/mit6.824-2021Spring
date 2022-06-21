package raft

func (rf *Raft) doHeartBroadcast() {
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.currentTerm != args.Term {
				// re-election, ignore
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

			}
		}(i)
	}
}
