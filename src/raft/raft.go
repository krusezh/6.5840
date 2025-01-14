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
	//	"bytes"
	"context"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

type Entry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	isLeader  bool
	heartbeat bool

	currentTerm int
	votedFor    int
	log         []*Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isLeader
	rf.mu.Unlock()
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	rf.isLeader = false
	rf.heartbeat = true
	reply.Success = true
	rf.votedFor = args.LeaderId
	rf.currentTerm = args.Term
	if len(args.Entries) == 0 {
		DPrintf("%d receive heartbeat from %d term %d prelogindex %d prelogterm %d leadercommit %d loglen %d", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(rf.log))
	} else {
		DPrintf("%d append term %d entries to %d prev log index %d, prev log term %d, log len %d", args.LeaderId, args.Term, rf.me, args.PrevLogIndex, args.PrevLogTerm, len(rf.log))

		if args.PrevLogIndex > len(rf.log) ||
			(args.PrevLogIndex > 0 && args.PrevLogTerm != rf.log[args.PrevLogIndex-1].Term) {
			DPrintf("%d append term %d entries to %d prev log index %d, prev log term %d, log len %d", args.LeaderId, args.Term, rf.me, args.PrevLogIndex, args.PrevLogTerm, len(rf.log))
			reply.Success = false
			return
		}

		if args.PrevLogIndex < len(rf.log) && args.Entries[0].Term != rf.log[args.PrevLogIndex].Term {
			DPrintf("%d append term %d entries to %d prev log index %d, prev log term %d, log len %d", args.LeaderId, args.Term, rf.me, args.PrevLogIndex, args.PrevLogTerm, len(rf.log))
			// delete the existing entry and all that follow it
			rf.log = rf.log[:args.PrevLogIndex]
		}
	}

	if args.PrevLogIndex == len(rf.log) {
		// Append any new entries not already in the log
		rf.log = append(rf.log, args.Entries...)

		// update commitIndex
		// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		commitIndex := rf.commitIndex
		if args.LeaderCommit > commitIndex {
			if args.LeaderCommit > len(rf.log) {
				commitIndex = len(rf.log)
			} else {
				commitIndex = args.LeaderCommit
			}
		}

		for rf.commitIndex < commitIndex {
			DPrintf("%d update commitIndex %d to %d, send applyMsg", rf.me, rf.commitIndex, rf.commitIndex+1)
			rf.commitIndex += 1
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.commitIndex-1].Command,
				CommandIndex: rf.commitIndex,
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// check term
	// check vote id
	// check logs
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	if args.Term > rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.log))) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.heartbeat = true
	} else {
		if len(rf.log) > 0 {
			DPrintf("%d term %d request vote from %d got false, current term %d, voted %d, last log index %d, last log term %d, log len %d last log %#v",
				args.CandidateId, args.Term, rf.me, rf.currentTerm, rf.votedFor, args.LastLogIndex, args.LastLogTerm, len(rf.log), rf.log[len(rf.log)-1])
		} else {
			DPrintf("%d term %d request vote from %d got false, current term %d, voted %d, last log index %d, last log term %d, log len %d",
				args.CandidateId, args.Term, rf.me, rf.currentTerm, rf.votedFor, args.LastLogIndex, args.LastLogTerm, len(rf.log))
		}

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
	return ok
}

func (rf *Raft) retryAppend(i int) {
	reply := &AppendEntriesReply{}

	rf.mu.Lock()
	ind := rf.nextIndex[i]
	// log.Printf("%d retryAppend %d, next index %d log len %d", rf.me, i, ind, len(rf.log))
	if ind > len(rf.log) {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for {
		rf.mu.Lock()
		// log.Printf("%d retryAppend inner %d, next index %d log len %d record next %d", rf.me, i, ind, len(rf.log), rf.nextIndex[i])
		entrys := rf.log[ind-1:]
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Entries:      entrys,
			LeaderCommit: rf.commitIndex,
		}

		if ind-1 > 0 {
			args.PrevLogIndex = ind - 1
			args.PrevLogTerm = rf.log[ind-2].Term
		}
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(i, args, reply)
		for !ok {
			DPrintf("%d sendAppendEntries %#v to %d failed, retry", rf.me, args, i)
			time.Sleep(100 * time.Millisecond)
			ok = rf.sendAppendEntries(i, args, reply)
		}

		if reply.Success {
			// change index
			rf.mu.Lock()
			end := ind + len(args.Entries) - 1
			if end > rf.matchIndex[i] {
				rf.matchIndex[i] = end
			}
			v := nthLargest(rf.matchIndex, len(rf.peers)/2)
			DPrintf("%d appendEntry to %d  %#v major index %d", rf.me, i, args, v)

			if v > rf.commitIndex && rf.log[v-1].Term == rf.currentTerm {
				for rf.commitIndex < v {
					DPrintf("%d update commitIndex %d to %d, send applyMsg", rf.me, rf.commitIndex, rf.commitIndex+1)
					rf.commitIndex += 1
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.log[rf.commitIndex-1].Command,
						CommandIndex: rf.commitIndex,
					}
				}
			}
			ind = end + 1
			if ind > len(rf.log) {
				if ind > rf.nextIndex[i] {
					rf.nextIndex[i] = ind
				}
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		} else {
			if reply.Term > args.Term {
				return
			}
			rf.mu.Lock()
			ind -= 1
			rf.mu.Unlock()
		}
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
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.isLeader
	if rf.isLeader {
		DPrintf("%d receive command", rf.me)
		defer func() {
			DPrintf("%d return start %d %d %v", rf.me, index, term, isLeader)
		}()
		entry := &Entry{Term: rf.currentTerm, Command: command}
		rf.log = append(rf.log, entry)
		index = len(rf.log)

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go rf.retryAppend(i)
		}
	}

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	ms := 50 + (rand.Int63() % 300)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	for rf.killed() == false {
		// Your code here (3A)
		DPrintf("%d ticker", rf.me)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.heartbeat {
			rf.heartbeat = false
		} else {
			// is leader: send heartbeat and sleep
			if rf.isLeader {
				DPrintf("%d is leader, send heartbeat", rf.me)
				for i := range rf.peers {
					if i == rf.me {
						continue
					}
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: len(rf.log),
						LeaderCommit: rf.commitIndex,
					}
					if len(rf.log) > 0 {
						args.PrevLogTerm = rf.log[len(rf.log)-1].Term
					}
					reply := &AppendEntriesReply{}

					go func(i int) {
						rf.sendAppendEntries(i, args, reply)
						if !reply.Success {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							if reply.Term > rf.currentTerm {
								rf.isLeader = false
								rf.votedFor = -1
								return
							}
						}
					}(i)
				}
				rf.mu.Unlock()
				// sleep heartbeat interval
				ms := 100 + (rand.Int63() % 50)
				// DPrintf("%d sleep heartbeat interval %v", rf.me, ms)
				time.Sleep(time.Duration(ms) * time.Millisecond)
				continue
			} else {
				// not leader: start election
				rf.currentTerm += 1
				rf.votedFor = rf.me
				term := rf.currentTerm
				rf.mu.Unlock()
				DPrintf("%d did not receive heartbeat, start election in term %d", rf.me, term)
				voted := 0
				noVoted := 0

				resps := make(chan bool, len(rf.peers))
				for i := range rf.peers {
					if i == rf.me {
						resps <- true
						continue
					}
					args := &RequestVoteArgs{
						Term:        term,
						CandidateId: rf.me,
						// log info
						LastLogIndex: len(rf.log),
					}
					if len(rf.log) > 0 {
						args.LastLogTerm = rf.log[len(rf.log)-1].Term
					}
					reply := &RequestVoteReply{}
					go func(i int, args *RequestVoteArgs, reply *RequestVoteReply) {
						rf.sendRequestVote(i, args, reply)
						if reply.VoteGranted {
							DPrintf("%d term %d request vote from %d, got %v", rf.me, args.Term, i, reply.VoteGranted)
						}
						resps <- reply.VoteGranted
					}(i, args, reply)
				}

				beLeader := false
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			LOOP:
				for {
					select {
					case resp := <-resps:
						if resp {
							voted += 1
							if voted > len(rf.peers)/2 {
								beLeader = true
								break LOOP
							}
						} else {
							noVoted += 1
							if noVoted > len(rf.peers)/2 {
								break LOOP
							}
						}
					case <-ctx.Done():
						// noVoted = len(rf.peers)
						DPrintf("%d term %d request vote timeout", rf.me, term)
						break LOOP
					}
				}
				cancel()

				rf.mu.Lock()
				if beLeader {
					DPrintf("%d is chosen to be the leader", rf.me)
					// should send heartbeat before set isLeader flag
					rf.isLeader = true
					rf.nextIndex = make([]int, len(rf.peers))
					// initial nextIndex
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log) + 1
					}
					rf.matchIndex = make([]int, len(rf.peers))
					rf.mu.Unlock()
					continue
				} else {
					DPrintf("%d failed to be the leader", rf.me)
					// reset info
					rf.currentTerm -= 1
					rf.votedFor = -1
				}
			}
			// else {
			// 	DPrintf("%d did not receive heartbeat, voted for %d", rf.me, rf.votedFor)
			// }
		}

		rf.nextIndex = rf.nextIndex[:0]
		rf.mu.Unlock()
		DPrintf("%d random election sleep", rf.me)
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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

	// Your initialization code here (3A, 3B, 3C).
	rf.votedFor = -1
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func nthLargest(nums []int, n int) int {
	if n <= 0 || n > len(nums) {
		return 0
	}

	// 创建一个副本来排序
	sortedNums := make([]int, len(nums))
	copy(sortedNums, nums)

	// 排序副本
	sort.Sort(sort.Reverse(sort.IntSlice(sortedNums)))

	// 返回第 n 大的数
	return sortedNums[n-1]
}
