package raft

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Printf("vote request: term %d;  %d request to be voted\n", args.Term, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dVote, "S%d C%d asking vote", rf.me, args.CandidateId)

	if args.Term < rf.currentTerm { // ignore
		// fmt.Println(rf.me, " ignore")
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		Debug(dVote, "S%d Term is higher than C%d, refuse it", rf.me, args.CandidateId)
		return
	} else if args.Term > rf.currentTerm { // turn to follower
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		Debug(dVote, "S%d Term is lower than C%d, turn to follower && reset voted_for", rf.me, args.CandidateId)
		rf.currentTerm, rf.votedFor = args.Term, voted_nil
		rf.TurnTo(follower)
		// can vote now
	}

	// log judge
	// TODO
	// RPC 实现了这样的限制：
	// RPC 中包含了候选人的日志信息，然后投票人会拒绝掉那些日志没有自己新的投票请求。

	if rf.votedFor == voted_nil || rf.votedFor == args.CandidateId { // haven't voted
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		//  prevent election timeouts (§5.2)
		rf.electionTimer.Reset(rf.election_timeout())
		// fmt.Printf("%d voted for %d\n", rf.me, args.CandidateId)
		Debug(dVote, "S%d Granting Vote to S%d at T%d", rf.me, rf.votedFor, rf.currentTerm)
		return
	}
	// have voted
	// fmt.Println(rf.me, " haven't voted")
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	Debug(dVote, "S%d Have voted to S%d at T%d, refuse S%d", rf.me, rf.votedFor, rf.currentTerm, args.CandidateId)
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dLog, "S%d S%d appendEntries", rf.me, args.LeaderId)

	if args.Term < rf.currentTerm { // leader out, refuse
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm { // turn to follower
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		rf.currentTerm, rf.votedFor = args.Term, voted_nil
		rf.TurnTo(follower)
	} else { // args.Term == rf.currentTerm
		if rf.status != follower {
			// If AppendEntries RPC received from new leader:
			// convert to follower
			rf.currentTerm = args.Term
			rf.TurnTo(follower)
		}
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	//  prevent election timeouts (§5.2)
	rf.electionTimer.Reset(rf.election_timeout())
}
