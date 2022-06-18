package raft

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Printf("vote request: term %d;  %d request to be voted\n", args.Term, args.CandidateId)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { // ignore
		// fmt.Println(rf.me, " ignore")
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm { // turn to follower
		rf.currentTerm = args.Term
		rf.TurnTo(follower)
	}

	// log judge

	if rf.votedFor == voted_nil || rf.votedFor == args.CandidateId { // haven't voted
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.electionTimer.Reset(rf.election_timeout())
		// fmt.Printf("%d voted for %d\n", rf.me, args.CandidateId)
		return
	}
	// have voted
	// fmt.Println(rf.me, " haven't voted")
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { // leader out, refuse
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.status != follower {
		rf.currentTerm = args.Term
		rf.TurnTo(follower)
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.electionTimer.Reset(rf.election_timeout())
}