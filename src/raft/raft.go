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
	"github.com/mhearttzw/mit/src/labgob"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "github.com/mhearttzw/mit/src/labrpc"

// import "bytes"
// import "labgob"

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
}

// three different states for any peer server
type State int

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

const HEARTBEAT_TIME = time.Duration(80) * time.Millisecond

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       State // state of raft peer
	LeaderId    int   // so follower can redirect clients

	// Persistent state on all servers:
	//(Updated on stable storage before responding to RPCs)
	currentTerm int   // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	voteFor     int // candidateId that received vote in current term (or null if none)
	log        [] LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//lastIncludedIndex int

	commitIndex int // 	index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	nextIndex  []int       // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int       // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// channel
	applyCh chan ApplyMsg

	// handle rpc, send committed msg to state machine to execute
	voteCh          chan struct{} // when raft server become leader form candidate
	appendEntriesCh chan struct{} // when raft server receive heartbeat from leader


}

// log entry
type LogEntry struct {
	Term    int
	Command interface{} // command for state machine
}

type AppendEntries struct {
	Index   int
	Term    int
	Command interface{}
	Log     LogEntry
}

// snapshot
//type Snapshot

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)

	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
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
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm, voteFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil {
	  DPrintf("ReadPersist error for server %v", rf.me)
	} else {
	  rf.currentTerm = currentTerm
	  rf.voteFor = voteFor
	  rf.log = log
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // own id for requesting vote
	LastLogIndex int // candidate server's last log entry index
	LastLogTerm  int // candidate server's last log entry term
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // vote server currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // conflict log index
	ConflictTerm  int  // conflict log term
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Term: %v, server %v receive server %v requestVote rpc.", rf.currentTerm, rf.me, args.CandidateId)
	currentTerm := rf.currentTerm

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	// would set voteFor = -1
	if args.Term > currentTerm {
		rf.beFollower(args.Term)
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	// basic leader election, allow at most one winner per term, each server gives out only one vote per term(persist on disk)
	// which guarantee that two different candidates can't accumulate majorities in same term
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer is more up-to-date.
	flag1 := (args.Term < currentTerm) || ((rf.voteFor != -1) && (rf.voteFor != args.CandidateId)) ||
		(args.LastLogTerm < rf.getLastLogTerm()) || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex())

	if !flag1 {
		reply.VoteGranted = true

		rf.beFollower(args.Term)
		rf.voteFor = args.CandidateId
		// persist (2C)
		rf.persist()
		send(rf.voteCh) // reset timeout
		DPrintf("bababab")
	}

	return
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()


	currentTerm := rf.currentTerm
	logSize := len(rf.log)
	reply.Term = currentTerm
	reply.Success = false

	// All servers;
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > currentTerm {
		rf.beFollower(args.Term)
	}

	// reset timeout
	send(rf.appendEntriesCh)

	/*
	1. Reply false if term < currentTerm (§5.1)
	**/
	flag := (args.Term < currentTerm)
	if flag {
		return
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// accelerated log backtracking optimization 
	prevLogTerm := -1
	if args.PrevLogIndex >= 0 && args.PrevLogIndex < logSize {
		prevLogTerm = rf.log[args.PrevLogIndex].Term
	}
	if prevLogTerm != args.PrevLogTerm {
		// If a follower does not have prevLogIndex in its log,
		// it should return with conflictIndex = len(log) and conflictTerm = None.
		if prevLogTerm == -1 {
			reply.ConflictIndex = logSize
			reply.ConflictTerm = -1
			return
		}
		// If a follower does have prevLogIndex in its log, but the term does not match,
		// it should return conflictTerm = log[prevLogIndex].Term, and then search its log for the first index whose entry has term equal to conflictTerm.
		for i := 0; i < logSize; i++ {
			if rf.log[i].Term == prevLogTerm {
				reply.ConflictIndex = i
				break
			}
		}
		reply.ConflictTerm = prevLogTerm
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	// 4. Append any new entries not already in the log
	//
	// todo need to figure out this part!
	// §5.5:
	// Raft RPCs are idempotent, so this causes no harm.
	// For example, if a follower receives an AppendEntries request that includes log entries already present in its log,
	// it ignores those entries in the new request.

	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	// persist (2C)
	rf.persist()

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, logSize-1)
		// All server rule :
		// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
		rf.updateLastAppliedIdx()
	}

	reply.Success = true
	return
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
	DPrintf("Term: %v, server %v send request vote to server %v args: %+v", rf.currentTerm, rf.me, server, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("Term: %v, server %v receive request vote from server %v reply: %+v", rf.currentTerm, rf.me, server, reply)

	return ok
}

// implement heartbeat
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("Term: %v, server %v send AppendEntries to server %v args: %+v", rf.currentTerm, rf.me, server, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("Term: %v, server %v receive AppendEntries from server %v；reply: %+v", rf.currentTerm, rf.me, server, reply)

	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = (rf.state == LEADER)
	if !isLeader {
		return index, term, isLeader
	}

	index = len(rf.log)

	// insert command into leader's log
	rf.log = append(rf.log, LogEntry{
		Term:    term,
		Command: command,
	})

	// persist (2C)
	rf.persist()

	// send msg to leader
	rf.sendHeartbeats()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
// create a background goroutine that will kick off leader election periodically by sending out RequestVote RPCs
// when it hasn't heard from another peer for a while.

// Make raft instance
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.voteFor = -1

	rf.log = make([]LogEntry, 1) 	// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	rf.voteCh = make(chan struct{}, 1)
	rf.appendEntriesCh = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("Server: %v, Initialize raft state, state: %v; currentTerm: %v; voteFor: %v", rf.me, rf.state, rf.currentTerm, rf.voteFor)
	// initialize state transition
	go func() {
		for {

			electionTimeOut := time.Duration(rand.Intn(100)+300) * time.Millisecond // set random electionTimeOut

			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()

			// periodically execute
			switch state {
			case CANDIDATE, FOLLOWER: // need to reset timeout, when receive vote or heartbeat, timeout reset
				select {
				case <-rf.voteCh: // receive higher term from leader, vote for it and become follower; become leader
					DPrintf("come into voteCh")
				case <-rf.appendEntriesCh: // when receive heartbeat, do not change state
				case <-time.After(electionTimeOut): // when times out, chang into candidate state
					DPrintf("server:%v, time after finished.", rf.me)
					go rf.beCandidate()
				}
			case LEADER: // period execute
/*				select {
			case <-rf.heartbeatCh:
				case <-time.After(HEARTBEAT_TIME):
					go rf.sendHeartbeats()
			}*/
				time.Sleep(HEARTBEAT_TIME)
				go rf.sendHeartbeats()
			}
		}
	}()

	return rf
}

// when server state become candidate, start new election when times out; 
// receive heartbeat and log entry
func (rf *Raft) beCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = CANDIDATE
	rf.currentTerm++
	rf.voteFor = rf.me

	// persist (2C)
	rf.persist()

	DPrintf("Term: %v; server %v become candidate", rf.currentTerm, rf.me)

	// start leader election
	rf.startElection()

}

// when server state become follower, receive heartbeat and log entry
func (rf *Raft) beFollower(term int) {
	//rf.mu.Lock()
	//defer rf.mu.Lock()

	rf.state = FOLLOWER
	rf.voteFor = -1
	rf.currentTerm = term

	// persist (2C)
	rf.persist()

	DPrintf("Term: %v; server %v become follower", rf.currentTerm, rf.me)

	return

}

// when server sate become candidate, send heartbeat
func (rf *Raft) beLeader() {
	send(rf.voteCh)
	DPrintf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~come into beleader!")
	if rf.state != CANDIDATE {
		DPrintf("Server: %v, NOT CANDIDATE ANY MORE!", rf.me)
		return
	}

	rf.state = LEADER
	// Volatile state on leaders:
	// (Reinitialized after election)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))


	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
	}

	DPrintf("Term: %v; server %v become leader!!!!!!!!!!!!!!!!!!!!!!!!!", rf.currentTerm, rf.me)

}

// leader election
func (rf *Raft) startElection() {
	var votes int = 1

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.getLastLogIndex(),
				LastLogTerm:  rf.getLastLogTerm(),
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(idx, args, reply)
			DPrintf("okokokokok: %v", ok)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				term := reply.Term
				if rf.currentTerm < term {
					rf.beFollower(term)
					send(rf.voteCh)
					return
				}
				if !ok || rf.state != CANDIDATE {
					DPrintf("Term: %v, server %v not candidate anymore; state %v", rf.currentTerm, rf.me, rf.state)
					return
				}

				DPrintf("okokokokok=============")
				if reply.VoteGranted {
					DPrintf("Term: %v, ------------server %v receive vote from server %v.", rf.currentTerm, rf.me, idx)
					votes++
				}
				DPrintf("Term: %v,------>>>>>>> server %v total votes: %v", rf.currentTerm, rf.me, votes)

				if votes > len(rf.peers)/2 {
					DPrintf("to beeeeee leader+++++++++++++++++++")
					rf.beLeader()
					rf.sendHeartbeats()
					send(rf.voteCh) // reset state loop

				}
			}

			return
		}(i)

	}

	DPrintf("Term: %v, server %v startElection function finished.", rf.currentTerm, rf.me)

}

// heartbeat; call AppendEntries RPC to replicate log
func (rf *Raft) sendHeartbeats() {

	for p, _ := range rf.peers {

		if p == rf.me {
			continue
		}

		DPrintf("Term: %v, server %v send heartbeat to server %v", rf.currentTerm, rf.me, p)
		go func(idx int) {
			//
			rf.mu.Lock()
			if rf.state != LEADER {
				DPrintf("Term: %v, server %v not leader anymore!!!!!!!!!!!!!!!!!!!!!!.", rf.currentTerm, rf.me)
				rf.mu.Unlock()
				return
			}

			// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
			// repeat during idle periods to prevent election timeouts (§5.2)
			entris := make([]LogEntry, 0)
			entris = append(entris, rf.log[rf.nextIndex[idx]:]...)
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.getPrevLogIndex(idx),
				PrevLogTerm:  rf.getPrevLogTerm(idx),
				Entries:      entris,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()


			reply := &AppendEntriesReply{}

			DPrintf("dduduuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu")
			DPrintf("Term: %v, server %v log: %v, rf.nextIndex[idx]:%v, args.Entries: %v", rf.currentTerm, rf.me, rf.log, rf.nextIndex[idx], len(entris))

			ok := rf.sendAppendEntries(idx, args, reply)

			// lock attention
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok || rf.state != LEADER {
				return
			}
			// All server: If RPC request or response contains term T > currentTerm:
			// set currentTerm = T, convert to follower (§5.1)
			if reply.Term > rf.currentTerm {
				rf.beFollower(reply.Term)
				return
			}

			// If last log index ≥ nextIndex for a follower: send
			// AppendEntries RPC with log entries starting at nextIndex
			//• If successful: update nextIndex and matchIndex for
			// follower (§5.3)
			//• If AppendEntries fails because of log inconsistency:
			// decrement nextIndex and retry (§5.3)
			// accelerated log backtracking optimization
			if !reply.Success {
				if len(args.Entries) == 0 {
					return
				}
				// when reply.ConflictTerm == 1 which means follower'log length is shorter than args.prevLogIndex
				rf.nextIndex[idx] = reply.ConflictIndex
				if reply.ConflictTerm != -1 {
					for i := args.PrevLogIndex; i >= 1; i-- {
						if rf.log[i-1].Term == reply.ConflictTerm {
							rf.nextIndex[idx] = i
							break
						}
					}
				}
				return
			}

			// reply success
			rf.updateNextMatchIdx(idx, args.PrevLogIndex + len(args.Entries))

		}(p)
	}

}

// Helper function
func (rf *Raft) getPrevLogIndex(idx int) int {
	return rf.nextIndex[idx] - 1
}

func (rf *Raft) getPrevLogTerm(idx int) int {
	prevLogIdx := rf.getPrevLogIndex(idx)
	if prevLogIdx < 0 {
		return -1
	}
	return rf.log[prevLogIdx].Term
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	idx := rf.getLastLogIndex()
	if idx < 0 {
		return -1
	}
	return rf.log[idx].Term
}

func (rf *Raft) updateNextMatchIdx(server int, matchIdx int) {
	rf.nextIndex[server] = matchIdx + 1
	rf.matchIndex[server] = matchIdx
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).

	rf.matchIndex[rf.me] = len(rf.log) - 1
	copyMatchIdx := make([]int, len(rf.matchIndex))
	copy(copyMatchIdx, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIdx)))
	N := copyMatchIdx[len(copyMatchIdx)/2]
	DPrintf("------------------------Term: %v, server: %v, N: %v, log: %v", rf.currentTerm, rf.me, N, rf.log)
	if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
		rf.commitIndex = N
		DPrintf("Term: %v, server: %v update commitIndex: %v", rf.currentTerm, rf.me, rf.commitIndex)
		// All server rule :
		// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
		rf.updateLastAppliedIdx()
	}
}

// All server rule :
// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
func (rf *Raft) updateLastAppliedIdx() {

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command: rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
}


// channel send function
func send(ch chan struct{}) {
	select {
	case <-ch: // if already set, consume it then resent it to avoid block
		DPrintf("channel function!")
	default:
	}
	ch <- struct{}{}
}
