package raft

import (
	"bytes"

	"6.824/labgob"
	"6.824/utils"
)

func (rf *Raft) RaftPersistSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) raftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(rf.log) != nil ||
		e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil {
		utils.Debug(utils.DError, "S%d encode fail", rf.me)
		panic("encode fail")
	}
	data := w.Bytes()
	return data
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.raftState())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var log []Entry
	var currentTerm, votedFor int

	if d.Decode(&log) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil {
		utils.Debug(utils.DError, "S%d decode fail", rf.me)
		panic("encode fail")
	}

	// log at least is 1
	rf.log = make([]Entry, len(log))
	copy(rf.log, log)
	rf.lastApplied = rf.frontLogIndex()
	rf.commitIndex = rf.frontLogIndex()
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
}

func (rf *Raft) persistSnapshot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.raftState(), snapshot)
}
