package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	logLock   sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	ApplyChan chan ApplyMsg

	// Your data here (3A, 3B, 3C).
	Status      int32
	Term        int32
	Log         []Log
	VoteFor     int
	IsHeatBeat  bool
	commitIndex int32
	lastApplied int32
	nextIndex   []int32
	matchIndex  []int32
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) GetLog(index int32) Log {
	rf.logLock.Lock()
	defer rf.logLock.Unlock()
	return rf.Log[index]
}

func (rf *Raft) AppendLog(log Log) {
	rf.logLock.Lock()
	defer rf.logLock.Unlock()
	rf.Log = append(rf.Log, log)
}

func (rf *Raft) GetLogLength() int {
	rf.logLock.Lock()
	defer rf.logLock.Unlock()
	return len(rf.Log)
}

func (rf *Raft) AppendLogByIndex(index int32, logs []Log) {
	rf.logLock.Lock()
	defer rf.logLock.Unlock()
	rf.Log = append(rf.Log[:index], logs...)
}

func (rf *Raft) GetAllLog() []Log {
	rf.logLock.Lock()
	defer rf.logLock.Unlock()
	copyLog := make([]Log, len(rf.Log))
	copy(copyLog, rf.Log)
	return copyLog
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	term = int(atomic.LoadInt32(&rf.Term))
	isleader = atomic.LoadInt32(&rf.Status) == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.Term)
	e.Encode(rf.GetAllLog())
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var Term int32
	var log []Log
	var commitIndex int32
	var lastApplied int32
	if d.Decode(&Term) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil {
		rf.Print("readPersist error")
	} else {
		rf.Term = Term
		rf.Log = log
		rf.commitIndex = commitIndex
		rf.lastApplied = lastApplied
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.Term {
		reply.Ok = false
		reply.Term = rf.Term
		return
	}
	defer func() {
		if args.Term > rf.Term {
			rf.Term = args.Term
		}
		rf.persist()
	}()
	if args.LastLogIndex < rf.commitIndex {
		reply.Ok = false
		reply.Term = rf.Term
		return
	}
	if args.LastLogIndex > rf.commitIndex {
		reply.Ok = true
		reply.Term = rf.Term
		return
	}

	if args.LastLogTerm < rf.GetLog(args.LastLogIndex).Term {
		reply.Ok = false
		reply.Term = rf.Term
		return
	}

	if args.Term > rf.Term {
		rf.InitStatus(Follower)
		rf.Term = args.Term
		reply.Ok = true
		reply.Term = rf.Term
		return
	}

	if args.CandidateId == rf.VoteFor {
		rf.InitStatus(Follower)
		reply.Ok = true
		reply.Term = rf.Term
		return
	}

	reply.Ok = false
	reply.Term = rf.Term
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.Term {
		reply.Ok = false
		reply.Term = rf.Term
		return
	}
	if args.Term > rf.Term {
		rf.Term = args.Term
	}
	if args.PrevLogIndex >= int32(rf.GetLogLength()) {
		reply.Ok = false
		reply.Term = rf.Term
		return
	}
	if args.PrevLogIndex >= 0 && rf.GetLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Ok = false
		reply.Term = rf.Term
		return
	}
	rf.InitStatus(Follower)
	rf.IsHeatBeat = true
	rf.AppendLogByIndex(args.PrevLogIndex+1, args.Entries)
	if args.LeaderCommitIndex < int32(rf.GetLogLength()) {
		rf.commitIndex = args.LeaderCommitIndex
	}
	rf.persist()
	reply.Ok = true
	return
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	res := make(chan bool, 0)
	go func() {
		res <- rf.peers[server].Call("Raft.RequestVote", args, reply)
	}()
	select {
	case v := <-res:
		return v
	case <-time.After(50 * time.Millisecond):
		return false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	res := make(chan bool, 0)
	go func() {
		res <- rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}()
	select {
	case v := <-res:
		return v
	case <-time.After(50 * time.Millisecond):
		return false
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (3B).
	if rf.Status != Leader {
		return index, term, false
	}
	rf.AppendLog(Log{
		Term:    rf.Term,
		Command: command,
	})
	index = rf.GetLogLength() - 1
	term = int(rf.Term)
	rf.persist()
	return index, term, rf.Status == Leader
}

func (rf *Raft) leaderSendAppendEntries(sever int) {
	for rf.killed() == false {
		if rf.Status != Leader {
			return
		}
		log := rf.GetAllLog()
		args := &AppendEntriesArgs{
			Term:              rf.Term,
			LeaderId:          int32(rf.me),
			PrevLogIndex:      rf.nextIndex[sever] - 1,
			PrevLogTerm:       rf.GetLog(rf.nextIndex[sever] - 1).Term,
			Entries:           log[rf.nextIndex[sever]:],
			LeaderCommitIndex: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}
		if rf.sendAppendEntries(sever, args, reply) {
			if reply.Ok {
				rf.nextIndex[sever] = int32(len(log))
				rf.matchIndex[sever] = int32(len(log)) - 1
				for N := rf.GetLogLength() - 1; N > int(rf.commitIndex); N-- {
					count := 1
					for i := range rf.peers {
						if rf.matchIndex[i] >= int32(N) && rf.Log[N].Term == rf.Term {
							count++
						}
					}
					if count*2 > len(rf.peers) {
						rf.commitIndex = int32(N)
						rf.persist()
						break
					}
				}
				return
			}
			if reply.Term > rf.Term {
				rf.Term = reply.Term
				rf.InitStatus(Follower)
				return
			}
			rf.nextIndex[sever] = 1
		}
	}
}

func (rf *Raft) ApplyCommand() {
	for rf.killed() == false {
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			rf.ApplyChan <- ApplyMsg{
				CommandValid: true,
				Command:      rf.GetLog(rf.lastApplied).Command,
				CommandIndex: int(rf.lastApplied),
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 200 + (rand.Int63() % 800)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		switch rf.Status {
		case Follower:
			if rf.IsHeatBeat {
				rf.IsHeatBeat = false
				break
			}
			if rf.VoteFor != -1 {
				break
			}
			rf.Candidate()
		case Candidate:
			rf.Candidate()
		case Leader:
			rf.leaderSendHeatBeat()
		}
	}
}

func (rf *Raft) Candidate() {
	if rf.IsHeatBeat {
		return
	}
	startTime := time.Now().UnixMilli()
	var num int32
	num = 1
	rf.InitStatus(Candidate)
	atomic.AddInt32(&rf.Term, 1)
	group := sync.WaitGroup{}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		group.Add(1)
		go func(server int) {
			defer group.Done()
			for time.Now().UnixMilli()-startTime < 100 && rf.Status == Candidate {
				reply := &RequestVoteReply{}
				rf.sendRequestVote(server, &RequestVoteArgs{
					Term:         rf.Term,
					CandidateId:  rf.me,
					LastLogIndex: rf.commitIndex,
					LastLogTerm:  rf.GetLog(rf.commitIndex).Term,
				}, reply)
				if reply.Ok {
					atomic.AddInt32(&num, 1)
					return
				}
				if reply.Term > rf.Term {
					rf.InitStatus(Follower)
					rf.Term = reply.Term
					return
				}
			}
		}(i)
	}
	group.Wait()
	if rf.Status != Candidate {
		return
	}
	rf.Print(num)
	if num > int32(len(rf.peers)/2) {
		rf.InitStatus(Leader)
		rf.Print("success")
		rf.leaderSendHeatBeat()
	}
}

func (rf *Raft) leaderSendHeatBeat() {
	group := sync.WaitGroup{}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		group.Add(1)
		go func(server int) {
			defer group.Done()
			for {
				if rf.Status != Leader {
					return
				}
				rf.leaderSendAppendEntries(server)
				time.Sleep(100 * time.Millisecond)
			}
		}(i)
	}
	group.Wait()
}

func (rf *Raft) InitStatus(status int32) {
	switch status {
	case Follower:
		rf.Status = status
		rf.VoteFor = -1
		rf.IsHeatBeat = false
	case Candidate:
		rf.Status = status
		rf.VoteFor = rf.me
		rf.IsHeatBeat = false
	case Leader:
		rf.nextIndex = make([]int32, len(rf.peers))
		rf.matchIndex = make([]int32, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = int32(rf.GetLogLength())
			rf.matchIndex[i] = -1
		}
		rf.Status = status
	}
	rf.persist()
}

func (rf *Raft) Print(msg interface{}) {
	fmt.Println(fmt.Sprintf("id: %d, term: %d, status: %d, logLen:%d, commit: %d, commitTerm:%d, log: %v", rf.me, rf.Term, rf.Status, rf.GetLogLength(), rf.commitIndex, rf.GetLog(rf.commitIndex).Term, msg))
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (3A, 3B, 3C).
	rf.Status = Follower
	rf.mu = sync.Mutex{}
	rf.VoteFor = -1
	rf.Term = 0
	rf.IsHeatBeat = false
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int32, len(peers))
	rf.matchIndex = make([]int32, len(peers))
	rf.Log = append(rf.Log, Log{
		Term:    0,
		Command: nil,
	})
	rf.logLock = sync.Mutex{}
	rf.ApplyChan = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ApplyCommand()

	return rf
}
