package mr

import (
	"log"
	"math"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	lock sync.Mutex//并发， 锁定共享数据
	status int//0:finish, 1:map, 2:reduce
	nMap int//分配map数量
	nReduce int//分配reduce数量
	task map[int]Task//分配任务记录
	allocTask chan Task//待分配任务管道
}

type Task struct {
	Type int
	Index int
	File string
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
		status: 1,
		nMap: len(files),
		nReduce: nReduce,
		task: make(map[int]Task),
		allocTask: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}

	// Your code here.
	for i, file := range files {
		task := Task{
			Type: 1,
			Index: i,
			File: file,
		}
		c.task[task.Type] = task
		c.allocTask <- task
	}

	c.server()
	return &c
}
