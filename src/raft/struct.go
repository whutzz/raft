package raft

type RequestVoteArgs struct {
	Term         int32
	CandidateId  int
	LastLogIndex int32
	LastLogTerm  int32
}

type RequestVoteReply struct {
	Ok   bool
	Term int32
}

type HeatBeatArgs struct {
	Term int32
}

type HeatBeatReply struct {
	Ok   bool
	Term int32
}

type Log struct {
	Term    int32
	Command interface{}
}

type AppendEntriesArgs struct {
	Term              int32
	LeaderId          int32
	PrevLogIndex      int32
	PrevLogTerm       int32
	Entries           []Log
	LeaderCommitIndex int32
}

type AppendEntriesReply struct {
	Term int32
	Ok   bool
}
