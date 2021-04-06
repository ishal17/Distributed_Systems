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
	"encoding/gob"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Alive        bool
}

// log entry
type Log struct {
	Command     interface{}
	CommandTerm int
}

// constants
const (
	// types
	FOLLOWER  int = 1
	CANDIDATE int = 2
	LEADER    int = 3

	// timeouts
	electionT  = 350
	randT      = 100
	heartbeatT = 150
	serverT    = 50
	smallT     = 10
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	curType     int // follower, candidate or leader
	currentTerm int
	votedFor    int
	logs        []Log
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := (rf.curType == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.dead)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	d := gob.NewDecoder(r)
	var deadV int32
	var currentTermV int
	var votedForV int
	var logsV []Log

	if d.Decode(&deadV) != nil ||
		d.Decode(&currentTermV) != nil ||
		d.Decode(&votedForV) != nil ||
		d.Decode(&logsV) != nil {

		print("!!! Error while reading persist !!!")
	} else {
		rf.dead = deadV
		rf.currentTerm = currentTermV
		rf.votedFor = votedForV
		rf.logs = logsV
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	hasOldTerm := args.LastLogTerm < rf.logs[len(rf.logs)-1].CommandTerm
	hasOldIdx := (args.LastLogTerm == rf.logs[len(rf.logs)-1].CommandTerm) && args.LastLogIndex < len(rf.logs)-1

	if args.Term < rf.currentTerm || hasOldTerm || hasOldIdx {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.curType = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		rf.persist()
	}
	reply.Term = rf.currentTerm
}

//
// example AppendEntries arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

//
// example AppendEntries reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term          int
	Success       bool
	MismatchStart int
}

//
// example AppendEntries handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm || args.LeaderID == rf.me {
		reply.Success = false
	} else {
		rf.curType = FOLLOWER
		rf.currentTerm = args.Term

		if len(rf.logs) > args.PrevLogIndex && rf.logs[args.PrevLogIndex].CommandTerm == args.PrevLogTerm {
			rf.addEntries(args, reply)
			reply.Success = true
		} else {
			rf.findMismatch(args, reply)
			reply.Success = false
		}
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) addEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if len(args.Entries) > 0 {
		rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
		rf.persist()
	}

	if rf.commitIndex >= args.LeaderCommit {
		return
	}

	rf.commitIndex = len(rf.logs) - 1
	if rf.commitIndex > args.LeaderCommit {
		rf.commitIndex = args.LeaderCommit
	}
}

func (rf *Raft) findMismatch(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.PrevLogIndex >= len(rf.logs) {
		reply.MismatchStart = len(rf.logs)
	} else {
		reply.MismatchStart = args.PrevLogIndex
		for (reply.MismatchStart > 0) && (rf.logs[reply.MismatchStart].CommandTerm == rf.logs[args.PrevLogIndex].CommandTerm) {
			reply.MismatchStart--
		}
		reply.MismatchStart++
		// l := 0
		// r := args.PrevLogIndex
		// term := rf.logs[args.PrevLogIndex].CommandTerm
		// for l <= r {
		// 	m := l + (r-l)/2

		// 	if m <= 1 || (rf.logs[m].CommandTerm == term && rf.logs[m-1].CommandTerm < term) {
		// 		reply.MismatchStart = m
		// 		return
		// 	}

		// 	if rf.logs[m].CommandTerm < term {
		// 		l = m + 1
		// 	} else {
		// 		r = m - 1
		// 	}
		// }
	}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) changeType(newType int, newTerm int) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	rf.curType = newType
	rf.currentTerm = newTerm

	if newType == LEADER {
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = len(rf.logs)
		}
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.curType != LEADER {
		return -1, -1, false
	}

	rf.logs = append(rf.logs, Log{command, rf.currentTerm})
	rf.matchIndex[rf.me] = len(rf.logs)
	rf.persist()

	return len(rf.logs) - 1, rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.curType = FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.logs = []Log{Log{nil, rf.currentTerm}}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.serverFn(applyCh)
	go rf.candidateFn()
	go rf.leaderFn()

	return rf
}

func (rf *Raft) serverFn(applyCh chan ApplyMsg) {
	alive := rf.dead == 0
	for alive {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			if len(rf.logs) > rf.lastApplied {
				applyMsg := ApplyMsg{true, rf.logs[rf.lastApplied].Command, rf.lastApplied, true}
				applyCh <- applyMsg
			} else {
				break
			}
		}
		rf.mu.Unlock()
		alive = rf.dead == 0
		time.Sleep(serverT * time.Millisecond)
	}
}

func (rf *Raft) leaderFn() {
	alive := rf.dead == 0
	for alive {
		time.Sleep(heartbeatT * time.Millisecond)
		rf.mu.Lock()
		if rf.curType == LEADER {
			currentTerm := rf.currentTerm
			for index := range rf.peers {
				go rf.sendHeartbeats(index, currentTerm)
			}
			rf.updateCommits()
		}
		rf.mu.Unlock()
		alive = rf.dead == 0
		time.Sleep(smallT * time.Millisecond)
	}
}

func (rf *Raft) updateCommits() {
	n := rf.commitIndex + 1
	for n < len(rf.logs) && rf.logs[n].CommandTerm <= rf.currentTerm {
		if rf.logs[n].CommandTerm == rf.currentTerm {
			cnt := 0
			for i := range rf.peers {
				if rf.matchIndex[i] >= n {
					cnt++
				}
			}
			if cnt > len(rf.peers)/2 {
				rf.commitIndex = n
			}
		}
		n++
	}
}

func (rf *Raft) sendHeartbeats(idx, currentTerm int) {
	for idx != rf.me && rf.curType == LEADER {
		rf.mu.Lock()
		index := rf.nextIndex[idx]
		appendEntries := rf.logs[index:]
		pTerm := rf.logs[index-1].CommandTerm
		args := AppendEntriesArgs{currentTerm, rf.me, index - 1, pTerm, appendEntries, rf.commitIndex}
		reply := AppendEntriesReply{}
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(idx, &args, &reply)
		rf.mu.Lock()
		len := len(rf.logs)
		if ok {
			terminated := rf.updateInfo(&args, &reply, idx, len)
			if terminated {
				rf.mu.Unlock()
				break
			}
		}
		rf.mu.Unlock()
		time.Sleep(smallT * time.Millisecond)
	}
}

func (rf *Raft) updateInfo(args *AppendEntriesArgs, reply *AppendEntriesReply, idx, len int) bool {
	if rf.currentTerm != args.Term {
		return true
	}

	if reply.Success {
		rf.matchIndex[idx] = len - 1
		rf.nextIndex[idx] = len
		return true
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.curType = FOLLOWER
		rf.persist()
		return true
	}

	rf.nextIndex[idx] = reply.MismatchStart
	return false
}

func (rf *Raft) candidateFn() {
	var counterLock sync.Mutex
	alive := rf.dead == 0
	for alive {
		rf.mu.Lock()
		if rf.curType == FOLLOWER {
			rf.curType = CANDIDATE
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(electionT+rand.Intn(randT)) * time.Millisecond)

		rf.mu.Lock()
		if rf.curType == CANDIDATE {
			var lastTerm int
			lastIdx := len(rf.logs) - 1
			newTerm := rf.currentTerm + 1

			if lastIdx >= 0 {
				lastTerm = rf.logs[lastIdx].CommandTerm
			} else {
				lastTerm = 0
			}
			counter := 0

			for index := range rf.peers {
				go rf.askForVote(&counterLock, &counter, index, newTerm, lastIdx, lastTerm)
			}
		}
		rf.mu.Unlock()
		alive = rf.dead == 0
		time.Sleep(smallT * time.Millisecond)
	}
}

func (rf *Raft) askForVote(counterLock *sync.Mutex, counter *int, idx, requestTerm, lastIdx, lastTerm int) {
	args := RequestVoteArgs{requestTerm, rf.me, lastIdx, lastTerm}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(idx, &args, &reply)

	rf.mu.Lock()
	if reply.Term > rf.currentTerm { // someone has higher term so let's follow them
		rf.currentTerm = reply.Term
		rf.curType = FOLLOWER
		rf.persist()
	} else if reply.VoteGranted { // I've got vote
		if ok && (args.Term == rf.currentTerm) {
			counterLock.Lock()
			*counter++
			if *counter > len(rf.peers)/2 { // maybe I became a leader
				if rf.curType == CANDIDATE || rf.curType == FOLLOWER {
					rf.changeType(LEADER, requestTerm)
					rf.persist()
				}
			}
			counterLock.Unlock()
		}
	}
	rf.mu.Unlock()
}
