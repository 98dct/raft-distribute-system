package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type SortedKey []KeyValue

// for sorting by key.
func (a SortedKey) Len() int           { return len(a) }
func (a SortedKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortedKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//worker的作用：获取任务(map任务或者reduce任务)并执行任务
loop:
	for {
		task := GetTask() //调用rpc向coordinator获取任务
		if task == nil {
			//可能由于任务分配完成，没拉到任务,等待一会重新拉任务
			time.Sleep(2 * time.Millisecond)
			continue
		}
		fmt.Println("拉取到的任务是：", task)
		switch task.TaskType {
		case MapTask:
			DoMapTask(mapf, task) //做map任务，将结果输出到文件中
			callDone(task)        //通过rpc调用，在coordinator中将任务状态设置为已完成，便于协调者和工作者退出
		case ReduceTask:
			DoReduceTask(reducef, task)
			callDone(task)
		case WaitingTask:
			fmt.Println("所有任务都分发完了，please wait...")
			time.Sleep(time.Second)
		case ExitTask:
			fmt.Println("task.Id:", task.TaskId, "is tertminated...")
			break loop
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

// 向coordinator获取任务
func GetTask() *Task {

	args := TaskArgs{}
	reply := Task{}

	ok := call("Coordinator.ProduceTask", &args, &reply)
	if !ok {
		fmt.Println("call failed\n")
	}

	return &reply
}

func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	var intermediate []KeyValue
	fileName := task.FileSlice[0]
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		fmt.Printf("open file:%s failed:%v\n", fileName, err.Error())
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("read file:%s failed:%v\n", fileName, err.Error())
	}

	//返回一个文件的所有word的结构体切片  example:[{"aa":1,"bb":1,"AA":1}]
	intermediate = mapf(fileName, string(content))

	rn := task.ReduceNum

	//创建一个长度为rn的二维切片
	HashedKv := make([][]KeyValue, rn)
	for _, kvStruct := range intermediate {
		HashedKv[ihash(kvStruct.Key)%rn] = append(HashedKv[ihash(kvStruct.Key)%rn], kvStruct)
	}

	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)

		//json.NewEncoder(ofile).Encode(kv) 意思是将kv结构体数据以json的形式解析到ofile文件流中
		encoder := json.NewEncoder(ofile)
		for _, kvStruct := range HashedKv[i] {
			encoder.Encode(kvStruct)
		}
		ofile.Close()
	}
}

func DoReduceTask(reducef func(key string, values []string) string, task *Task) {
	id := task.TaskId
	intermediate := shuffle(task.FileSlice)
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		fmt.Println("fail to create temp file", err.Error())
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", id)
	os.Rename(tempFile.Name(), fn)
}

// 组合8个输入文件，输出[]KeyValue
func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filePath := range files {
		file, _ := os.Open(filePath)
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(SortedKey(kva))
	return kva
}

// 调用rpc在协调者中将任务状态设为已完成
func callDone(task *Task) Task {
	args := task
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &args, &reply)
	if !ok {
		fmt.Println("call failed")
	}
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
