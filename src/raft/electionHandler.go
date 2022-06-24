package raft

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Printf("vote request: term %d;  %d request to be voted\n", args.Term, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	defer rf.persist()

	Debug(dVote, "S%d C%d asking vote", rf.me, args.CandidateId)

	if args.Term < rf.currentTerm { // ignore
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		Debug(dVote, "S%d Term is higher than C%d, refuse it", rf.me, args.CandidateId)
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		rf.currentTerm, rf.votedFor = args.Term, voted_nil
		Debug(dVote, "S%d Term is lower than C%d, turn to follower && reset voted_for", rf.me, args.CandidateId)
		rf.TurnTo(follower)
		// can vote now
	}

	if rf.votedFor == voted_nil || rf.votedFor == args.CandidateId { // haven't voted
		// log judge
		if !rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
			reply.VoteGranted, reply.Term = false, rf.currentTerm
			Debug(dVote, "S%d C%d not uo-to-date, refuse it{arg:%+v, index:%d term:%d}", rf.me, args.CandidateId, args, rf.lastLogIndex(), rf.log[rf.lastLogIndex()].Term)
			return
		}
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
