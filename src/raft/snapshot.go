package raft

func (rf *Raft) doInstallSnapshot(peer int) {
	rf.mu.Lock()
	if rf.status != leader {
		Debug(dWarn, "S%d status change, it is not leader", rf.me)
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.frontLog().Index,
		LastIncludedTerm:  rf.frontLog().Term,
	}

	args.Data = make([]byte, rf.persister.SnapshotSize())
	copy(args.Data, rf.persister.ReadSnapshot())
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}

	ok := rf.sendInstallSnapshot(peer, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// status changed or outdue data, ignore
	if rf.currentTerm != args.Term || rf.status != leader || reply.Term < rf.currentTerm {
		// overdue, ignore
		Debug(dInfo, "S%d old response from C%d, ignore it", rf.me, peer)
		return
	}

	if reply.Term > rf.currentTerm {
		Debug(dTerm, "S%d S%d term larger(%d > %d)", rf.me, peer, args.Term, rf.currentTerm)
		rf.currentTerm, rf.votedFor = reply.Term, voted_nil
		rf.persist()
		rf.TurnTo(follower)
		return
	}

	rf.nextIndex[peer] = args.LastIncludedIndex + 1

	Debug(dInfo, "S%d send snapshot to C%d success!", rf.me, peer)
}
