package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mutex sync.Mutex

type Coordinator struct {
	// Your definitions here.
	reduceNum      int
	taskId         int
	distPhase      Phase      //处于的任务阶段
	taskReduceChan chan *Task //reduce任务chan 使用chan可以并发安全地获取任务
	taskMapChan    chan *Task //map任务chan
	allTask        TaskMeta   //存放这所有的task
	files          []string   //传入的文件数组
}

type TaskMeta struct {
	taskMetaMap map[int]*TaskMetaInfo
}

type TaskMetaInfo struct {
	state     State     //任务的状态
	task      *Task     //任务
	StartTime time.Time //任务开始时间，为crash做准备
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	mutex.Lock()
	defer mutex.Unlock()
	if c.distPhase == AllDone {
		fmt.Println("all tasks are finished,the coordinator will exit!!!")
		return true
	} else {
		return false
	}

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		reduceNum:      nReduce,
		distPhase:      MapPhase,
		taskReduceChan: make(chan *Task, nReduce),
		taskMapChan:    make(chan *Task, len(files)),
		allTask: TaskMeta{
			taskMetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
		files: files,
	}

	// Your code here.
	c.makeMapTasks(files)

	c.server()

	go c.CrashDetector()
	return &c
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(2 * time.Second)
		mutex.Lock()
		if c.distPhase == AllDone {
			mutex.Unlock()
			break
		}

		for _, v := range c.allTask.taskMetaMap {

			if v.state == Working && time.Since(v.StartTime) > 9*time.Second {
				fmt.Printf("the task id:%d is crash,take %d s\n", v.task.TaskId, time.Since(v.StartTime))

				//判断v的type，加入对应的chan重新交给worker执行
				switch v.task.TaskType {
				case MapTask:
					c.taskMapChan <- v.task
					v.state = Waiting
				case ReduceTask:
					c.taskReduceChan <- v.task
					v.state = Waiting
				}

			}

		}

		mutex.Unlock()

	}
}

// 生成maptask,并将maptask任务放入taskMapChan中，可以让多个map-worker并发从taskMapchan中获取任务
func (c *Coordinator) makeMapTasks(files []string) {
	for _, file := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:  MapTask,
			TaskId:    id,
			ReduceNum: c.reduceNum,
			FileSlice: []string{file},
		}

		//保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			state: Waiting,
			task:  &task,
		}
		c.allTask.acceptTaskMeta(&taskMetaInfo)
		fmt.Println("make a map task: ", task)
		c.taskMapChan <- &task
	}
}

// 生成reduceTask,并将reduceTask任务放入taskReduceChan中，让多个reduce-worker任务可以并发从taskReduceChan中获取
func (c *Coordinator) makeReduceTask() {
	for i := 0; i < c.reduceNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskType:  ReduceTask,
			TaskId:    id,
			FileSlice: selectReduceName(i),
		}

		taskMetaInfo := TaskMetaInfo{
			state: Waiting,
			task:  &task,
		}
		c.allTask.acceptTaskMeta(&taskMetaInfo)
		fmt.Println("make a reduce task: ", task)
		c.taskReduceChan <- &task
	}
}

func selectReduceName(reduceNum int) []string {
	var res []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, file := range files {
		//匹配reduce文件
		if strings.HasPrefix(file.Name(), "mr-tmp") && strings.HasSuffix(file.Name(), strconv.Itoa(reduceNum)) {
			res = append(res, file.Name())
		}
	}
	return res
}

func (c *Coordinator) generateTaskId() int {
	res := c.taskId
	c.taskId++
	return res
}

func (t *TaskMeta) acceptTaskMeta(taskMetaInfo *TaskMetaInfo) bool {
	id := taskMetaInfo.task.TaskId
	metaInfo, _ := t.taskMetaMap[id]
	if metaInfo != nil {
		fmt.Println("map contain taskId:", id)
		return false
	}
	t.taskMetaMap[id] = taskMetaInfo
	return true
}

func (c *Coordinator) ProduceTask(args *TaskArgs, reply *Task) error {

	//分发任务上锁，因为多个worker会同时调用这个方法
	mutex.Lock()
	defer mutex.Unlock()

	//根据所处的任务阶段生产任务
	switch c.distPhase {
	case MapPhase:
		if len(c.taskMapChan) > 0 {
			*reply = *<-c.taskMapChan
			if !c.allTask.judgeState(reply.TaskId) {
				fmt.Printf("map task id:%d is running", reply.TaskId)
			}
		} else {
			reply.TaskType = WaitingTask //任务分发完了，但是还没有完成
			if c.allTask.checkTaskDone() {
				//true：代表完成了  false：代表没完成
				c.toNextPhase()
			}
			return nil
		}
	case ReducePhase:
		if len(c.taskReduceChan) > 0 {
			*reply = *<-c.taskReduceChan
			if !c.allTask.judgeState(reply.TaskId) {
				fmt.Printf("reduce task id:%d is running", reply.TaskId)
			}
		} else {
			reply.TaskType = WaitingTask //任务分发完了，但是还没有完成
			if c.allTask.checkTaskDone() {
				//true：代表完成了  false：代表没完成
				c.toNextPhase()
			}
			return nil
		}
	case AllDone:
		reply.TaskType = ExitTask
	default:
		panic("undefined phase!!!")
	}

	return nil
}

// 默认处于waiting状态，修改状态为working  return true
// 如果已经处于working状态,return false
func (t *TaskMeta) judgeState(taskId int) bool {

	metaInfo, ok := t.taskMetaMap[taskId]
	if !ok || metaInfo.state != Waiting {
		return false
	}

	metaInfo.state = Working
	metaInfo.StartTime = time.Now()
	return true
}

func (t *TaskMeta) checkTaskDone() bool {

	var (
		mapDoneNum      = 0
		mapUndoneNum    = 0
		reduceDoneNum   = 0
		reduceUndoneNum = 0
	)

	//遍历任务map
	for _, taskMetaInfo := range t.taskMetaMap {

		if taskMetaInfo.task.TaskType == MapTask {
			if taskMetaInfo.state == Done {
				mapDoneNum++
			} else {
				mapUndoneNum++
			}
		} else if taskMetaInfo.task.TaskType == ReduceTask {
			if taskMetaInfo.state == Done {
				reduceDoneNum++
			} else {
				reduceUndoneNum++
			}
		}
	}

	if (mapDoneNum > 0 && mapUndoneNum == 0) && (reduceDoneNum == 0 && reduceUndoneNum == 0) {
		return true
	} else if reduceDoneNum > 0 && reduceUndoneNum == 0 {
		return true
	}

	return false
}

func (c *Coordinator) toNextPhase() {
	if c.distPhase == MapPhase {
		c.makeReduceTask()
		c.distPhase = ReducePhase
	} else if c.distPhase == ReducePhase {
		c.distPhase = AllDone
	}
}

// 将任务标记为完成
// 多个worker可能并发调用，需要加锁
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {

	mutex.Lock()
	defer mutex.Unlock()

	switch args.TaskType {
	case MapTask:
		meta, ok := c.allTask.taskMetaMap[args.TaskId]
		if ok && meta.state == Working {
			meta.state = Done
			fmt.Println("task id:", args.TaskId, "is finished")
		} else {
			fmt.Println("task id:", args.TaskId, "is already finished")
		}
		break
	case ReduceTask:
		meta, ok := c.allTask.taskMetaMap[args.TaskId]
		if ok && meta.state == Working {
			meta.state = Done
			fmt.Println("task id:", args.TaskId, "is finished")
		} else {
			fmt.Println("task id:", args.TaskId, "is already finished")
		}
		break
	default:
		panic("task type is undefined !!!")
	}

	return nil
}
