package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	//使用 pid 作为 WorkID
	WorkerID := strconv.Itoa(os.Getpid())

	var lastTaskType string
	var lastTaskNum int
	var lastTaskStatus int
	//进入循环，向 Coordinator 申请 task
	for {
		args := TaskArgs{
			WorkerID:       WorkerID,
			LastTaskType:   lastTaskType,
			LastTaskNum:    lastTaskNum,
			LastTaskStatus: lastTaskStatus,
		}
		reply := TaskReply{}
		call("Coordinator.Run", &args, &reply)
		//判断退出
		//reply.TaskType 为空，说明 Run 已经关闭
		if reply.CoordinatorStatus == "" {
			fmt.Println(fmt.Sprintf("Worker %v reviced sign", WorkerID))
			break
		}

		lastTaskNum = reply.TaskNum
		lastTaskType = reply.TaskType

		//MR处理
		if reply.TaskType == "M" {
			if err := mapHandle(WorkerID, &reply, mapf); err != nil {
				lastTaskStatus = 2
				fmt.Println(fmt.Sprintf("workID %v mapHandle error: %v", WorkerID, err))
			} else {
				lastTaskStatus = 1
			}
		} else if reply.TaskType == "R" {

		} else {
			lastTaskStatus = 2
		}
	}

	fmt.Println(fmt.Sprintf("Worker %v close", WorkerID))
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func mapHandle(id string, reply *TaskReply, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(reply.TaskInputFile)
	if err != nil {
		return err
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	//生成kv结果
	allKv := mapf(reply.TaskInputFile, string(content))
	hashedKv := make(map[int][]KeyValue)
	//所有kv分桶
	for _, kv := range allKv {
		hashNum := ihash(kv.Key) % reply.CoordinatorReduceNum
		hashedKv[hashNum] = append(hashedKv[hashNum], kv)
	}
	//中间文件生成
	for i := 0; i < reply.CoordinatorReduceNum; i++ {
		ofile, _ := os.Create(tmpMapOutFile(id, reply.TaskNum, i))
		for _, kv := range allKv {
			fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
		}
		ofile.Close()
	}
	return nil
}

func reduceHandle(id string, reply *TaskReply, reducef func(string, []string) string) error {
	var lines []string
	for i := 0; i < reply.CoordinatorMapNum; i++ {
		inputFile := finalMapOutFile(i, reply.TaskNum)
		file, err := os.Open(inputFile)
		if err != nil {
			fmt.Sprintf("Fail to open file %v, err %v \n", inputFile, err)
			return err
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			fmt.Sprintf("Fail to read file %v, err %v \n", inputFile, err)
			return err
		}
		lines = append(lines, strings.Split(string(content), "\n")...)
	}
	var kv []KeyValue
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		kv = append(kv, KeyValue{
			Key: parts[0],
			Value: parts[1],
		})
	}

	sort.Sort(ByKey(kv))

	ofile, _ := os.Create(tmpReduceOutFile(id, reply.TaskNum))

	i := 0
	for i < len(kv) {
		j := i + 1
		for j < len(kv) && kv[j].Key == kv[i].Key {
			var values []string
			for k := i; k < j; k++ {
				values = append(values, kv[k].Value)
			}
			output := reducef(kv[i].Key, values)

			// 写出至结果文件
			fmt.Fprintf(ofile, "%v %v\n", kv[i].Key, output)

			i = j
		}
		ofile.Close()
	}

	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock	()
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
