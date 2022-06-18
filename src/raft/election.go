package raft

func (rf *Raft) doElection() {
	votedcount := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: 0,
				LastLogTerm:  0,
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				// fmt.Println(rf.me, "not ok")
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// fmt.Printf("reply %v\n", reply)
			if reply.Term > rf.currentTerm {
				// turn to follower
				// fmt.Println(rf.me, " will be follow, vote fail")
				rf.currentTerm = reply.Term
				rf.TurnTo(follower)
				return
			}

			if reply.VoteGranted {
				votedcount++
				if votedcount > len(rf.peers)/2 && rf.status == candidate {
					rf.TurnTo(leader)
					// fmt.Println(rf.me, " will be leader, peer num: ", len(rf.peers))
				}
			}
		}(i)
	}
}

func (rf *Raft) doHeartBroadcast() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}

			ok := rf.sendAppendEntries(i, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.TurnTo(follower)
				return
			}

			if reply.Success {

			}
		}(i)
	}
}
