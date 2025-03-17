package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	//Mutex锁
	lock sync.Mutex

	//Reduce任务数量
	reduceCount int

	//Worker进程ID
	workers []int

	//当前状态, 0正在做map任务, 1正在做Reduce任务, 2等Worker全部退出
	status int

	//Map任务
	mapTasks map[string]*mapTask
	//Map任务已完成数量
	mapTaskDoneCount int

	//Reduce任务
	reduceTasks []*reduceTask
	//Reduce任务完成数量
	reduceTaskDoneCount int
}

// Reduce任务结构
type reduceTask struct {
	id       int
	working  bool
	done     bool
	workerID int
}

// Map任务结构
type mapTask struct {
	id       int
	name     string
	working  bool
	done     bool
	workerID int
}

// Your code here -- RPC handlers for the worker to call.
/*
1.分Map任务给worker,worker完成之后call一个task ok(10s计时)
2.全部Map完成后开始reduce,每个key的所有values传给worker
3.写入文件mr-out-x 先排序好所有的intermediate
4.关闭所有worker后退出自己
*/

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) MrTask(args *MrTaskArgs, reply *MrTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	//检查状态
	if c.status == 0 {
		//正在做map任务，找一个余下的map任务给worker
		mapName := ""
		for _, maptask := range c.mapTasks {
			if maptask.working == false && maptask.done == false {
				mapName = maptask.name
				maptask.working = true
				maptask.workerID = args.WorkerID
				break
			}
		}
		if mapName == "" {
			//没有找到可以做的map任务，传0等待
			reply.TaskType = 0
			return nil
		}
		//回传给worker数据
		reply.MapID = c.mapTasks[mapName].id
		reply.MapName = mapName
		reply.TaskType = 1
		reply.ReduceCount = c.reduceCount
		go c.checkTaskTimeout(1, mapName, 0)
	} else if c.status == 1 {
		//做Reduce任务
		reduceID := -1
		for i, reducetask := range c.reduceTasks {
			if reducetask.working == false && reducetask.done == false {
				//找到能做的reduce任务
				reduceID = i
				reducetask.workerID = args.WorkerID
				reducetask.working = true
				break
			}
		}
		if reduceID == -1 {
			//没有找到可以做的reduce任务，传0等待
			reply.TaskType = 0
			return nil
		}
		reply.TaskType = 2
		reply.ReduceID = reduceID
		reply.MapTaskCount = c.mapTaskDoneCount
		go c.checkTaskTimeout(2, "", reduceID)
	} else if c.status == 2 {
		//所有任务完成
		reply.Exit = true
	}
	return nil
}

func (c *Coordinator) checkTaskTimeout(taskType int, mapName string, reduceID int) {
	time.Sleep(5 * time.Second)
	c.lock.Lock()
	if taskType == 1 {
		if c.mapTasks[mapName].done == false {
			log.Printf("Map task[%v] dead, worker[%v] dead! ", mapName, c.mapTasks[mapName].workerID)
			c.mapTasks[mapName].working = false
			c.deleteWorker(c.mapTasks[mapName].workerID)
		}
	}
	if taskType == 2 {
		if c.reduceTasks[reduceID].done == false {
			log.Printf("Reduce task[%v] dead, worker[%v] dead! ", reduceID, c.reduceTasks[reduceID].workerID)
			c.reduceTasks[reduceID].working = false
			c.deleteWorker(c.reduceTasks[reduceID].workerID)
		}
	}
}

// 删除worker时 需要提前lock
func (c *Coordinator) deleteWorker(workerID int) {
	workerKey := -1
	for i, worker := range c.workers {
		if worker == workerID {
			workerKey = i
			break
		}
	}
	if workerKey == -1 {
		log.Printf("Worker [%v] exit error! worker is not exist or something wrong!", workerID)
	} else {
		c.workers = append(c.workers[:workerKey], c.workers[workerKey+1:]...)
	}
}

// WorkerExit Worker退出回传
func (c *Coordinator) WorkerExit(args *WorkerExitArgs) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	log.Printf("Worker [%v] exit!", args.WorkerID)
	c.deleteWorker(args.WorkerID)
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.TaskType == 1 {
		c.mapTasks[args.MapName].done = true
		c.mapTasks[args.MapName].working = false
		c.mapTaskDoneCount++
		log.Printf("Map task[%v] done!", args.MapName)
		if c.mapTaskDoneCount == len(c.mapTasks) {
			c.status = 1
			log.Printf("all map tasks done!")
		}
	} else if args.TaskType == 2 {
		c.reduceTasks[args.ReduceID].done = true
		c.reduceTasks[args.ReduceID].working = false
		c.reduceTaskDoneCount++
		log.Printf("Reduce task[%v] done!", args.ReduceID)
		if c.reduceTaskDoneCount == len(c.reduceTasks) {
			c.status = 2
			log.Printf("All reduce tasks done!")
		}
	}
	return nil
}

// RegisterWorker Worker访问此接口来注册到Coordinator,传回一个ID
func (c *Coordinator) RegisterWorker(workerID *int) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	*workerID = len(c.workers)
	c.workers = append(c.workers, *workerID)
	log.Printf("Worker [%v] register worker!", *workerID)
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
	ret := false
	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.status == 2 && len(c.workers) == 0 {
		log.Printf("Coordinator Done!")
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	//initialize coordinator
	c.reduceCount = nReduce
	c.workers = make([]int, 0)
	c.status = 0
	//initialize Map task
	c.mapTasks = make(map[string]*mapTask)
	for i, fileName := range files {
		c.mapTasks[fileName] = &mapTask{i, fileName, false, false, 0}
	}
	//initialize Reduce task
	c.reduceTasks = make([]*reduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &reduceTask{i, false, false, 0}
	}

	c.server()
	return &c
}
