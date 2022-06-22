package raft

type Entry struct {
	Term int
	Cmd  interface{}
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) isUpToDate(lastLogIndex int, lastLogTerm int) bool {
	index := rf.lastLogIndex()
	term := rf.log[index].Term
	if term == lastLogTerm {
		return lastLogIndex >= index
	}
	return lastLogTerm > term
}

func (rf *Raft) toCommit() {
	for i := rf.lastLogIndex(); i >= rf.commitIndex; i-- {
		if rf.log[i].Term != rf.currentTerm {
			return
		}

		cnt := 1 // 1 => self
		for j, match := range rf.matchIndex {
			if j != rf.me && match >= i {
				cnt++
			}
			if cnt > len(rf.peers)/2 {
				rf.commitIndex = i
				Debug(dCommit, "S%d commit to %v", rf.me, rf.commitIndex)
				go rf.applyLog()
				return
			}
		}
	}
}
