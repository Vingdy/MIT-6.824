package mr

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	lock      sync.Mutex      //并发， 锁定共享数据
	status    string          //F:finish, M:map, R:reduce
	nMap      int             //分配map数量
	nReduce   int             //分配reduce数量
	task      map[string]Task //分配任务记录
	allocTask chan Task       //待分配任务管道
}

//Work既处理map也处理reduce
type Task struct {
	TaskType  string
	TaskNum   int
	InputFile string

	AllocatedWorkerID string
	TaskDeadline      time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		status:    "M",
		nMap:      len(files),
		nReduce:   nReduce,
		task:      make(map[string]Task),
		allocTask: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}

	// Your code here.
	for i, file := range files {
		task := Task{
			TaskType:  "M",
			TaskNum:   i,
			InputFile: file,
		}
		c.task[GenID(task.TaskType, task.TaskNum)] = task
		c.allocTask <- task
	}

	//Coordinator 启动，开启 rpc 服务器等待 Worker 请求
	fmt.Println("Coordinator start")
	c.server()

	//Task 回收

	return &c
}

func GenID(t string, num int) string {
	return fmt.Sprintf("task-%v-%v", t, num)
}

type TaskArgs struct {
	//主要用于判断上一次 Task 运行情况
	LastTaskType string
	LastTaskNum  int

	WorkerID string
}

type TaskReply struct {
	//记录当前 Task 相关数据
	TaskType  string
	TaskNum   int
	InputFile string

	//记录总 MR 相关数据
	Map    int
	Reduce int
}

//简单 rpc， 准备好内容放置到 args 和 reply 后返回请求方
func (c *Coordinator) Run(args *TaskArgs, reply *TaskReply) error {
	//之前已经有 Task 运行过了
	if args.LastTaskType != "" {

	}

	//查看还有无待分配任务
	task, ok := <-c.allocTask
	//无任务分配，MR 完成，结束
	if !ok {
		fmt.Println("All MR finish, done run")
		return nil
	}

	//锁定 c 的内容，因为请求可能并发
	c.lock.Lock()
	defer c.lock.Unlock()

	//记录当次 task
	fmt.Println(fmt.Sprintf("alloc %v task %v to worker %v", task.TaskType, task.TaskNum, args.WorkerID))
	task.AllocatedWorkerID = args.WorkerID
	task.TaskDeadline = time.Now().Add(10 * time.Second)
	c.task[GenID(task.TaskType, task.TaskNum)] = task

	//放置响应数据
	reply.TaskType = task.TaskType
	reply.TaskNum = task.TaskNum
	reply.InputFile = task.InputFile
	reply.Map = c.nMap
	reply.Reduce = c.nReduce

	return nil
}
