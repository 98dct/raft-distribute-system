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
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

//节点的角色  leader/follower/candicator
type Status int

//投票的状态 2A
type VoteState int

//追加日志的状态 2A、2B
type AppendEntriesState int

//全局心跳超时时间
var HeartBeatTimeout = 120 * time.Millisecond

//跟随者、竞选者、领导者
const (
	Follower Status = iota
	Candidate
	Leader
)

const (
	Normal VoteState = iota //投票过程正常
	Killed                  //Raft节点已终止
	Expire                  //投票(消息/竞选者)过期
	Voted                   //本Term内已经投过票
)

const (
	AppNormal    AppendEntriesState = iota //追加正常
	AppOutOfDate                           // 追加过时
	AppKilled                              //Raft程序终止
	AppRepeat                              //追加重复(2B)
	AppCommited                            //追加的日志已提交(2B)
	Mismatch                               //追加不匹配(2B)
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

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

	//所有的servers 拥有的变量
	currentTerm int        //当前任期
	votedFor    int        //当前任期把票投给了谁
	logs        []LogEntry //日志条目组，包含了状态机要执行的指令集，以及收到领导时的任期号

	//所有的servers经常修改的：
	//正常情况下commitIndex和lastApplied应该是一致的，但是如果有一个新的提交，但是还未被应用的话，lastApplied应该要小一点
	commitIndex int //状态机中已知的被提交的日志条目的索引值 初始化为0，持续递增（0---n）
	lastApplied int //最后一个被追加到状态机日志的索引值

	//leader拥有的可见变量，用来管理他的follower
	//nextIndex与matchIndex初始化长度应该为len(peers),leader对于每个follower都记录它的nextIndex和matchIndex
	//nextIndex指的是下一个appendEntries要从哪里开始
	//matchIndex值的是已知的某follower的log与leader的log最大匹配到第几个index，已经apply
	nextIndex  []int //对于每一个server，需要发送给他下一个日志条目的索引值（初始化为leader日志index+1,那么范围就对标len）
	matchIndex []int //对于每一个server，已经复制给该server的最后日志条目下标

	//自己追加的
	status   Status        //该节点是什么角色（状态）
	overtime time.Duration //设置超时时间 200-400ms
	timer    *time.Ticker  //每个节点的定时器

	applyChan chan ApplyMsg //日志都是存在这里client取
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//由leader复制log条目，也可以当作是心跳连接，注释中的rf为leader节点
type AppendEntriesArgs struct {
	Term         int        //leader的任期
	LeaderId     int        //leader自身的ID
	PrevLogIndex int        //预计从哪里追加index，因此每次比当前的len(logs)多1  初始化为rf.nextIndex[i]-1
	PrevLogTerm  int        //追加新的日志的任期号（当前leader的任期号）  rf.currentTerm
	Entries      []LogEntry //存储的日志  为空时就是心跳
	LeaderCommit int        //leader的commit index指的是最后一个被大多数机器都复制的日志index
}

type AppendEntriesReply struct {
	Term     int                //leader的term可能是过时的，此时收到的term用于更新他自己
	Success  bool               //只有follower与Args中的PrevLogIndex/PrevLogTerm都匹配才会接过去新的日志，不匹配直接返回false
	AppState AppendEntriesState //追加状态
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool

	term = rf.currentTerm
	if rf.status == Leader {
		isleader = true
	} else {
		isleader = false
	}

	// Your code here (2A).
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的任期
	CandidateId  int //候选人的id
	LastLogIndex int //候选人日志条目的最后索引
	LastLogTerm  int //候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int       //投票方的term，如果候选人比自己还低，就改为这个
	VoteGranted bool      //是否投票给了该候选人
	VoteState   VoteState //投票状态
}

//
// example RequestVote RPC handler.
// 个人认为定时刷新的地方应该是别的节点与当前的节点在数据上不冲突的时候就要刷新
// 因为如果不是数据冲突那么定时相当于防止自身去选举的一个心跳
// 如果是因为数据冲突，那么这个节点不用刷新定时是为了当前整个raft能尽快有个正确的leader
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//当前节点crash
	if rf.killed() {
		reply.VoteState = Killed
		reply.Term = -1
		reply.VoteGranted = false
		return
	}

	//出现网络分区，该竞选者已经OutOfDate(过时)
	if args.Term < rf.currentTerm {
		reply.VoteState = Expire
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		//重置自身的状态
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// 此时比自己任期小的都已经把票还原
	if rf.votedFor == -1 {

		currentLogIndex := len(rf.logs) - 1
		currentLogTerm := 0
		//如果currentLogIndex下标不是-1就把term赋值过来
		if currentLogIndex >= 0 {
			currentLogTerm = rf.logs[currentLogIndex].Term
		}

		//论文中第二个匹配条件，当前的peer要复合arg两个参数的预期
		if args.LastLogIndex < currentLogIndex || args.LastLogTerm < currentLogTerm {
			reply.VoteState = Expire
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}

		//投票
		rf.votedFor = args.CandidateId

		reply.VoteState = Normal
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		rf.timer.Reset(rf.overtime)
	} else {
		// 只剩下任期相同，但是票已经给了，此时存在两种情况
		reply.VoteState = Voted
		reply.VoteGranted = false

		// 1.当前的节点来自同一轮，不同竞选者，但是票数已经给了
		if rf.votedFor != args.CandidateId {
			//告诉reply票已经没了，返回false
			return
		} else { //2.当前节点的票已经给同一个人了，但是由于sleep等网络原因，又发送了一次请求
			//重置自身的状态
			rf.status = Follower
		}

		rf.timer.Reset(rf.overtime)

	}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNums *int) bool {

	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	for !ok {
		//失败重传
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)

	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//由于网络分区，请求投票的人的term比自己的还小，不给予投票
	if args.Term < rf.currentTerm {
		return false
	}

	//对reply的返回情况进行分支处理
	switch reply.VoteState {
	//消息过期的两种情况
	//1.本身的term过期了比节点还小
	//2.请求投票的节点的日志的条目落后于被要求投票的节点了
	case Expire:
		rf.status = Follower
		rf.timer.Reset(rf.overtime)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		}
	case Normal, Voted:
		if reply.VoteGranted && reply.Term == rf.currentTerm && *voteNums <= (len(rf.peers)/2) {
			*voteNums++
		}

		//票数超一半
		if *voteNums >= (len(rf.peers)/2)+1 {

			*voteNums = 0
			//本身就是leader在网络分区中更具有话语权的leader
			if rf.status == Leader {
				return ok
			}

			//本身不是leader，那么需要初始化nextIndex数组
			rf.status = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs) + 1
			}
			rf.timer.Reset(HeartBeatTimeout)
		}
	case Killed:
		return false
	}

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

	return index, term, isLeader
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
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.status {
			case Follower:
				rf.status = Candidate
				fallthrough

			case Candidate:
				//初始化自身的任期，并把票投给自己
				rf.currentTerm += 1
				rf.votedFor = rf.me
				votedNums := 1 //统计自身的票数

				//每轮选举开始后，重新设置选举超时
				rf.overtime = time.Duration(150+rand.Intn(200)) * time.Second
				rf.timer.Reset(rf.overtime)

				//对自身以外的节点发起投票请求
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					voteArgs := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.logs) - 1,
						LastLogTerm:  0,
					}
					if len(rf.logs) > 0 {
						voteArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
					}

					voteReply := RequestVoteReply{}
					go rf.sendRequestVote(i, &voteArgs, &voteReply, &votedNums)

				}
			case Leader:
				//进行心跳和日志同步
				appendNums := 1 //对于正确返回的节点数量
				rf.timer.Reset(HeartBeatTimeout)
				//构造msg
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					appendEntriesArgs := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}

					appendEntriesReply := AppendEntriesReply{}
					go rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply, &appendNums)

				}
			}

			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNums *int) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}

	//必须加在这里，否则加在前面retry进入时，RPC也需要一个锁，但是又获取不到
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//对reply的返回状态进行分支
	switch reply.AppState {

	//目标节点crash
	case AppKilled:
		return false

	//目标节点正常返回
	case AppNormal:
		//2A的test目的是让leader能不能连续任期，所以2A只需要对节点初始化返回即可
		return true
	//出现网络分区，该leader已经OutOfDate
	case AppOutOfDate:
		//该节点变成追随者，并重置rf状态
		rf.status = Follower
		rf.votedFor = -1
		rf.timer.Reset(rf.overtime)
		rf.currentTerm = reply.Term
	}

	return ok
}

// AppendEntries 建立心跳、同步日志RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//节点crash
	if rf.killed() {
		reply.AppState = AppKilled
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//对当前的rf进行ticker重置
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.status = Follower
	rf.timer.Reset(rf.overtime)

	//对返回的reply进行赋值
	reply.AppState = AppNormal
	reply.Term = rf.currentTerm
	reply.Success = true
	return
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
	rf.applyChan = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.status = Follower
	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Second
	rf.timer = time.NewTicker(rf.overtime)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
