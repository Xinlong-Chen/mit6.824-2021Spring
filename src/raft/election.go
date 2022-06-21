package raft

func (rf *Raft) doElection() {
	votedcount := 1
	// not another goroutine, needn't lock it
	// might timeout,
	// then lead to send different term vote request
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				// fmt.Println(rf.me, "not ok")
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.currentTerm != args.Term || rf.status != candidate {
				// election timeout, re-election
				// ignore it
				return
			}

			// If RPC request or response contains term T > currentTerm:
			// set currentTerm = T, convert to follower (§5.1)
			if reply.Term > rf.currentTerm {
				// turn to follower
				// fmt.Println(rf.me, " will be follow, vote fail")
				rf.currentTerm, rf.votedFor = reply.Term, voted_nil
				rf.TurnTo(follower)
				return
			}

			if reply.VoteGranted {
				votedcount++
				// If votes received from majority of servers: become leader
				if votedcount > len(rf.peers)/2 && rf.status == candidate {
					rf.TurnTo(leader)
					// fmt.Println(rf.me, " will be leader, peer num: ", len(rf.peers))
				}
			}
		}(i)
	}
}

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
