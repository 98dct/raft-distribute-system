package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Task struct {
	TaskType  TaskType //任务类型  map 、reduce
	TaskId    int      //任务id
	ReduceNum int      //传入的reduce的数量，用于hash
	FileSlice []string //传入的文件名
}

// rpc参数  worker只是获取一个任务  不需要任何字段
type TaskArgs struct{}

// 任务类型
type TaskType int

// 任务阶段
type Phase int

// 任务状态
type State int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask //任务都分发完了，但是还没有执行完，阶段没改变
	ExitTask
)

const (
	MapPhase    Phase = iota //分发map任务阶段
	ReducePhase              //分发reduce任务阶段
	AllDone                  //所有已完成
)

const (
	Working State = iota //任务处于工作状态
	Waiting              //任务处于等待状态
	Done                 //任务处于完成状态
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
