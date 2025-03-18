package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type Key []KeyValue

// 实现 sort.Interface 接口
func (a Key) Len() int {
	return len(a)
}
func (a Key) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a Key) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerID := register()
	args := MrTaskArgs{WorkerID: workerID}
	for {
		reply := MrTaskReply{}
		ok := call("Coordinator.MrTask", &args, &reply)
		if !ok {
			log.Fatalf("worker get task failed!")
		}
		if reply.Exit == false {
			if reply.TaskType == 0 {
				//time.Sleep(5 * time.Second)
			} else if reply.TaskType == 1 {
				mapResult := mapf(reply.MapName, readFile(reply.MapName))
				//任务拆分
				reduceContent := make([][]KeyValue, reply.ReduceCount)
				for _, kv := range mapResult {
					key := ihash(kv.Key) % reply.ReduceCount
					reduceContent[key] = append(reduceContent[key], kv)
				}
				for i, content := range reduceContent {
					fileName := fmt.Sprintf(intermediateFileName, reply.MapID, i)
					f, _ := os.Create(fileName)
					enc := json.NewEncoder(f)
					for _, line := range content {
						enc.Encode(&line)
					}
					f.Close()
				}
				taskDone(1, reply.MapName, 0)
			} else if reply.TaskType == 2 {
				inter := make([]KeyValue, 0)
				for i := 0; i < reply.MapTaskCount; i++ {
					fileName := fmt.Sprintf(intermediateFileName, i, reply.ReduceID)
					reduceF, _ := os.Open(fileName)
					dec := json.NewDecoder(reduceF)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						inter = append(inter, kv)
					}
					reduceF.Close()
				}
				sort.Sort(Key(inter))

				fileName := fmt.Sprintf(outputFileName, reply.ReduceID)
				outF, _ := os.Create(fileName)
				i := 0
				for i < len(inter) {
					j := i + 1
					for j < len(inter) && inter[j].Key == inter[i].Key {
						j++
					}
					var values []string
					for k := i; k < j; k++ {
						values = append(values, inter[k].Value)
					}
					output := reducef(inter[i].Key, values)
					fmt.Fprintf(outF, "%v %v\n", inter[i].Key, output)
					i = j
				}
				outF.Close()
				taskDone(2, "", reply.ReduceID)
			}
		} else {
			workerExit(workerID)
			os.Exit(1)
		}
	}
}

func workerExit(workerID int) {
	args := WorkerExitArgs{workerID}
	ok := call("Coordinator.WorkerExit", &args, &None{})
	if !ok {
		log.Fatalf("worker[%v] exit failed!", workerID)
	}
}

func taskDone(taskType int, mapName string, reduceID int) {
	args := TaskDoneArgs{TaskType: taskType, MapName: mapName, ReduceID: reduceID}
	ok := call("Coordinator.TaskDone", &args, &None{})
	if !ok {
		log.Fatalf("worker get task failed!")
	}
}

func readFile(fileName string) string {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("open file %s failed!", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("read file %s failed!", fileName)
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("close file %s failed!", fileName)
	}
	return string(content)
}

func register() int {
	var workerID int
	ok := call("Coordinator.RegisterWorker", &None{}, &workerID)
	if !ok {
		log.Fatalf("Worker register to coordinator failed!")
	}
	return workerID
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

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
