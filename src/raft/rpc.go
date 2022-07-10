package raft

import "6.824/utils"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // for fast backup
	XIndex  int
	XLen    int
}

type InstallSnapshotArgs struct {
	// Your data here (2A, 2B).
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	utils.Debug(utils.DInfo, "S%d send RequestVote request to %d {%+v}", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		utils.Debug(utils.DWarn, "S%d call (RequestVote)rpc to C%d error", rf.me, server)
		return ok
	}
	utils.Debug(utils.DInfo, "S%d get RequestVote response from %d {%+v}", rf.me, server, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	utils.Debug(utils.DInfo, "S%d send AppendEntries request to %d {%+v}", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		utils.Debug(utils.DWarn, "S%d call (AppendEntries)rpc to C%d error", rf.me, server)
		return ok
	}
	utils.Debug(utils.DInfo, "S%d get AppendEntries response from %d {%+v}", rf.me, server, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	utils.Debug(utils.DInfo, "S%d send InstallSnapshot request to %d {%+v}", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		utils.Debug(utils.DWarn, "S%d call (InstallSnapshot)rpc to C%d error", rf.me, server)
		return ok
	}
	utils.Debug(utils.DInfo, "S%d get InstallSnapshot response from %d {%+v}", rf.me, server, reply)
	return ok
}
