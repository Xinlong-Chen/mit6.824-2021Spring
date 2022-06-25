package raft

const (
	// magic number
	voted_nil int = -10086
)

// ticker() call doElection(), ticker() hold lock
func (rf *Raft) doElection() {
	votedcount := 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.log[rf.lastLogIndex()].Term,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
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
				Debug(dTerm, "S%d S%d term larger(%d > %d)", rf.me, i, args.Term, rf.currentTerm)
				// turn to follower
				rf.currentTerm, rf.votedFor = reply.Term, voted_nil
				rf.persist()
				rf.TurnTo(follower)
				return
			}

			if reply.VoteGranted {
				votedcount++
				// If votes received from majority of servers: become leader
				if votedcount > len(rf.peers)/2 && rf.status == candidate {
					rf.TurnTo(leader)
				}
			}
		}(i)
	}
}
