package raft

import "6.824/utils"

type Entry struct {
	Index int
	Term  int
	Cmd   interface{}
}

func (rf *Raft) frontLog() Entry {
	return rf.log[0]
}

func (rf *Raft) frontLogIndex() int {
	return rf.log[0].Index
}

func (rf *Raft) lastLog() Entry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) lastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// nextIndex max is len(log), will out of range
func (rf *Raft) transfer(index int) (int, int) {
	begin := rf.frontLogIndex()
	end := rf.lastLogIndex()
	// left open, right close
	// fuck range!
	if index < begin || index > end {
		utils.Debug(utils.DWarn, "S%d log out of range: %d, [%d, %d]", rf.me, index, begin, end)
		return 0, -1
	}
	return index - begin, 0
}

func (rf *Raft) getEntry(index int) (Entry, int) {
	begin := rf.frontLogIndex()
	end := rf.lastLogIndex()
	// left open, right close
	// fuck range!
	if index < begin || index > end {
		utils.Debug(utils.DWarn, "S%d log out of range: %d, [%d, %d]", rf.me, index, begin, end)
		return Entry{magic_index, magic_term, nil}, -1
	}
	return rf.log[index-begin], 0
}

func (rf *Raft) isUpToDate(lastLogIndex int, lastLogTerm int) bool {
	entry := rf.lastLog()
	index := entry.Index
	term := entry.Term
	if term == lastLogTerm {
		return lastLogIndex >= index
	}
	return lastLogTerm > term
}

func (rf *Raft) toCommit() {
	// append entries before commit
	if rf.commitIndex >= rf.lastLogIndex() {
		return
	}

	for i := rf.lastLogIndex(); i > rf.commitIndex; i-- {
		entry, err := rf.getEntry(i)
		if err < 0 {
			continue
		}

		if entry.Term != rf.currentTerm {
			return
		}

		cnt := 1 // 1 => self
		for j, match := range rf.matchIndex {
			if j != rf.me && match >= i {
				cnt++
			}
			if cnt > len(rf.peers)/2 {
				rf.commitIndex = i
				utils.Debug(utils.DCommit, "S%d commit to %v", rf.me, rf.commitIndex)
				rf.applyCond.Signal()
				return
			}
		}
	}

	utils.Debug(utils.DCommit, "S%d don't have half replicated from %v to %v now", rf.me, rf.commitIndex, rf.lastLogIndex())
}

func (rf *Raft) HasLogInCurrentTerm() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := len(rf.log) - 1; i > 0; i-- {
		if rf.log[i].Term > rf.currentTerm {
			continue
		}
		if rf.log[i].Term == rf.currentTerm {
			return true
		}
		if rf.log[i].Term < rf.currentTerm {
			break
		}
	}
	return false
}
