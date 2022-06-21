package raft

type Entry struct {
	Term int
	Cmd  interface{}
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) toCommit() {
	for i := rf.lastLogIndex(); i >= rf.commitIndex; i-- {
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
