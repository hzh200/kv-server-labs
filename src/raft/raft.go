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
	"errors"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
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

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	applyCh           chan ApplyMsg       // Channel to submit committed log entry

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent states
	currentTerm       int
	votedFor          int
	log               []LogEntry
	// Volatile states
	commitIndex       int
	lastApplied       int
	// Volatile states (Leaders only)
	nextIndex         []int
	matchIndex        []int
	// Volatile states (Followers only)
	receivedHeartbeat bool

	// Reserved snapshot
	snapshot          []byte
	// snapshotCommnd   string
	snapshotIndex     int
	snapshotTerm      int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	if rf.nextIndex != nil {
		isleader = true
	} else {
		isleader = false
	}
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	if rf.snapshot != nil {
		r := bytes.NewBuffer(rf.snapshot)
		d := labgob.NewDecoder(r)
		var command int
		d.Decode(&command)
		e.Encode(command)
		e.Encode(rf.snapshotIndex)
		e.Encode(rf.snapshotTerm)
	}
	
	rf.persister.Save(w.Bytes(), rf.snapshot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	
	var command int
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&command) != nil || d.Decode(&snapshotIndex) != nil || d.Decode(&snapshotTerm) != nil {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(command)
	rf.snapshot = w.Bytes()
	rf.snapshotIndex = snapshotIndex
	rf.snapshotTerm = snapshotTerm
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	if index <= rf.snapshotIndex {
		return
	}
	if index > len(rf.log) + rf.snapshotIndex {
		return
	}
	
	rf.snapshot = snapshot
	rf.snapshotTerm = rf.log[index - rf.snapshotIndex - 1].Term
	rf.log = rf.log[index - rf.snapshotIndex:]
	rf.snapshotIndex = index
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term          int
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term          int
	VoteGranted   bool
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term          int
	LeaderId      int
	PrevLogIndex  int
	PrevLogTerm   int
	Entries       []LogEntry
	LeaderCommit  int
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term          int
	Success       bool
}

// AppendEntries RPC arguments structure.
type InstallSnapshotArgs struct {
	Term               int
	LeaderId           int
	LastIncludedIndex  int
	LastIncludedTerm   int
	// Offset             int
	Data               []byte
	// Done               bool
}

// AppendEntries RPC reply structure.
type InstallSnapshotReply struct {
	Term          int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	} 

	// if request Term > currentTerm, update currentTerm for self.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.nextIndex = nil
		rf.matchIndex = nil
	}

	
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	entry, _ := rf.getLogEntry(len(rf.log) + rf.snapshotIndex)

	if entry.Term > args.LastLogTerm {
		reply.VoteGranted = false
		return
	}

	if entry.Term == args.LastLogTerm && len(rf.log) + rf.snapshotIndex > args.LastLogIndex {
		reply.VoteGranted = false
		return
	}
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if request Term > currentTerm, update currentTerm of self.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.nextIndex = nil
		rf.matchIndex = nil
	}
	
	rf.receivedHeartbeat = true
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	} 
	
	if len(rf.log) + rf.snapshotIndex < args.PrevLogIndex {
		reply.Success = false
		return
	}

	if args.PrevLogIndex != 0 {
		if entry, err := rf.getLogEntry(args.PrevLogIndex); err != nil || entry.Term != args.PrevLogTerm {
			reply.Success = false
			return
		}
	}

	if args.PrevLogIndex < rf.snapshotIndex {
		reply.Success = false
		return
	}

	rf.log = rf.log[:args.PrevLogIndex - rf.snapshotIndex]
	rf.log = append(rf.log, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		end := args.LeaderCommit
		if args.LeaderCommit > len(rf.log) + rf.snapshotIndex {
			end = len(rf.log) + rf.snapshotIndex
		}
		for i := rf.commitIndex + 1; i <= end; i++ {
			entry, _ := rf.getLogEntry(i)
			rf.applyCh <- ApplyMsg{
				CommandValid: true, 
				Command: entry.Command, 
				CommandIndex: i,
			}
		}
		rf.commitIndex = end
	}

	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
	}
	reply.Success = true
	rf.persist()
}

// InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.currentTerm = args.Term
	rf.snapshot = args.Data
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	
	rf.commitIndex = args.LastIncludedIndex
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot: args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm: args.LastIncludedTerm,
	}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// if response Term > currentTerm, update currentTerm of self.
	if reply.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.nextIndex = nil
		rf.matchIndex = nil
	}
	return ok
}

// code to send a AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// if response Term > currentTerm, update currentTerm of self.
	if reply.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.nextIndex = nil
		rf.matchIndex = nil
	}
	return ok
}

// code to send a InstallSnapshot RPC to a server.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	// if response Term > currentTerm, update currentTerm of self.
	if reply.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.nextIndex = nil
		rf.matchIndex = nil
	}
	return ok
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
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader = rf.GetState()

	if !isLeader {
		return index, term, isLeader
	}

	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	index = len(rf.log) + rf.snapshotIndex
	rf.persist()
	return index, term, isLeader
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
	// rf.persist()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// pause for a random amount of time between 50 and 350 milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		ms := 200 + (rand.Int63() % 150) // between 200 and 350 miliseconds.
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Your code here (3A)
		// Check if a leader election should be started.
		if _, isLeader := rf.GetState(); !rf.receivedHeartbeat && !isLeader {
			rf.mu.Lock()
			rf.currentTerm++			
			// vote for self.
			rf.votedFor = rf.me
			rf.mu.Unlock()
			voteCount := 1
			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.log) + rf.snapshotIndex
			entry, err := rf.getLogEntry(args.LastLogIndex)
			if err == nil {
				args.LastLogTerm = entry.Term
			}
			
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				reply := RequestVoteReply{}
				if rf.sendRequestVote(i, &args, &reply) {
					if reply.VoteGranted {
						voteCount++
					}
				}
			}

			// server considers itself as a leader. 
			if voteCount > len(rf.peers) / 2 {
				rf.mu.Lock()
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log) + rf.snapshotIndex + 1
					rf.matchIndex[i] = 0
				}
				rf.mu.Unlock()
			}
		}
		rf.mu.Lock()
		rf.receivedHeartbeat = false
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeater() {
	for !rf.killed() {
		if _, isLeader := rf.GetState(); !isLeader {
			time.Sleep(time.Duration(10) * time.Millisecond)
			continue
		}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			if rf.nextIndex == nil {
				break
			}
			if rf.snapshotIndex != 0 && rf.snapshotIndex >= rf.nextIndex[i] - 1 {
				args := InstallSnapshotArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.snapshotIndex
				args.LastIncludedTerm = rf.snapshotTerm
				args.Data = rf.snapshot
				reply := InstallSnapshotReply{}
				rf.sendInstallSnapshot(i, &args, &reply)
			}

			reply := AppendEntriesReply{Success: false}
			for {
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				if rf.nextIndex == nil {
					break
				}
				args.PrevLogIndex = rf.nextIndex[i] - 1
				entry, err := rf.getLogEntry(args.PrevLogIndex)
				if err == nil {
					args.PrevLogTerm = entry.Term
				}
				if args.PrevLogIndex < len(rf.log) + rf.snapshotIndex {
					if args.PrevLogIndex < rf.snapshotIndex {
						args.Entries = rf.log[:]
					} else {
						args.Entries = rf.log[args.PrevLogIndex - rf.snapshotIndex:]
					}
				}
				
				if !rf.sendAppendEntries(i, &args, &reply) {
					break
				}

				if reply.Success {
					break
				}

				if rf.nextIndex == nil || rf.nextIndex[i] == 1 {
					break
				}
				rf.mu.Lock()
				rf.nextIndex[i]--
				rf.mu.Unlock()
			}
			if reply.Success {
				rf.mu.Lock()
				if rf.nextIndex != nil {
					rf.nextIndex[i] = len(rf.log) + rf.snapshotIndex + 1
					rf.matchIndex[i] = len(rf.log) + rf.snapshotIndex
					rf.matchIndex[rf.me] = len(rf.log) + rf.snapshotIndex
				}
				rf.mu.Unlock()
			}
		}

		// update commit consensus logs.
		rf.mu.Lock()
		if rf.nextIndex != nil {
			matchIndex := make([]int, len(rf.peers))
			copy(matchIndex, rf.matchIndex)
			sort.Slice(matchIndex, func(i, j int) bool {
				return matchIndex[i] < matchIndex[j]
			})
			n := matchIndex[len(matchIndex) - (len(rf.peers) / 2 + 1)]
			entry, err := rf.getLogEntry(n)
			if n > rf.commitIndex && err == nil && entry.Term == rf.currentTerm {
				if n >= rf.snapshotIndex {
					start := rf.commitIndex + 1
					if rf.snapshot != nil && rf.commitIndex < rf.snapshotIndex {
						rf.applyCh <- ApplyMsg{
							SnapshotValid: true,
							Snapshot: rf.snapshot,
							SnapshotIndex: rf.snapshotIndex,
							SnapshotTerm: rf.snapshotTerm,
						}
						start = rf.snapshotIndex + 1
					}
					for i := start; i <= n; i++ {
						entry, _ := rf.getLogEntry(i)
						rf.applyCh <- ApplyMsg{
							CommandValid: true, 
							Command: entry.Command, 
							CommandIndex: i,
						}
					}
				}
				rf.commitIndex = n
			}
		}

		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (rf *Raft) getLogEntry(index int) (LogEntry, error) {
	if index > len(rf.log) + rf.snapshotIndex || index == 0 {
		return LogEntry{}, errors.New("")
	}
	if index > rf.snapshotIndex {
		return rf.log[index - rf.snapshotIndex - 1], nil
	} else {
		r := bytes.NewBuffer(rf.snapshot)
		d := labgob.NewDecoder(r)
		var command string
		d.Decode(&command)
		return LogEntry{Command: command, Term: rf.snapshotTerm}, nil
	}
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
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.receivedHeartbeat = false
	rf.snapshot = nil
	rf.snapshotIndex = 0
	rf.snapshotTerm = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start heartbeater goroutine to send heartbeats
	go rf.heartbeater()
	
	return rf
}
