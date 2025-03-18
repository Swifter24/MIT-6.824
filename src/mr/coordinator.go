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
		reply.ReduceCount = c.reduceCount //Worker 知道 Reduce 任务的总数，以便正确地进行数据分区
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
		reply.MapTaskCount = c.mapTaskDoneCount //确保 Reduce 任务在所有 Map 任务完成后才开始，避免 Reduce 任务读取不完整的数据
		go c.checkTaskTimeout(2, "", reduceID)
	} else if c.status == 2 {
		//所有任务完成
		reply.Exit = true
	}
	return nil
}

func (c *Coordinator) checkTaskTimeout(taskType int, mapName string, reduceID int) {
	timer := time.NewTimer(15 * time.Second)
	<-timer.C
	c.lock.Lock()
	defer c.lock.Unlock()

	// 确保任务仍未完成再执行超时处理
	if taskType == 1 {
		if task, exists := c.mapTasks[mapName]; exists && !task.done && task.working {
			log.Printf("Map task [%v] timeout, reassigning...", mapName)
			task.working = false
			c.deleteWorker(task.workerID)
		}
	}
	if taskType == 2 {
		if reduceID >= 0 && reduceID < len(c.reduceTasks) {
			task := c.reduceTasks[reduceID]
			if !task.done && task.working {
				log.Printf("Reduce task [%v] timeout, reassigning...", reduceID)
				task.working = false
				c.deleteWorker(task.workerID)
			}
		}
	}
}

func (c *Coordinator) deleteWorker(workerID int) {
	workerKey := -1
	for i, worker := range c.workers {
		if worker == workerID {
			workerKey = i
			break
		}
	}
	if workerKey == -1 {
		log.Printf("Worker [%v] exit error! worker is not exist or already removed!", workerID)
		return
	}
	// 从 workers 列表移除
	c.workers = append(c.workers[:workerKey], c.workers[workerKey+1:]...)
	log.Printf("Worker [%v] removed from coordinator!", workerID)
}

func (c *Coordinator) WorkerExit(args *WorkerExitArgs, n *None) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	log.Printf("Worker [%v] exit!", args.WorkerID)
	c.deleteWorker(args.WorkerID)
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, n *None) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if args.TaskType == 1 {
		if task, exists := c.mapTasks[args.MapName]; exists && !task.done {
			task.done = true
			task.working = false
			c.mapTaskDoneCount++
			log.Printf("Map task [%v] done!", args.MapName)
		}
		// 确保所有 Map 任务完成后，才修改状态
		if c.mapTaskDoneCount == len(c.mapTasks) {
			c.status = 1
			log.Printf("All map tasks done! Starting Reduce phase.")
		}
	} else if args.TaskType == 2 {
		if args.ReduceID >= 0 && args.ReduceID < len(c.reduceTasks) {
			task := c.reduceTasks[args.ReduceID]
			if !task.done {
				task.done = true
				task.working = false
				c.reduceTaskDoneCount++
				log.Printf("Reduce task [%v] done!", args.ReduceID)
			}
		}
		// 确保所有 Reduce 任务完成后，才修改状态
		if c.reduceTaskDoneCount == len(c.reduceTasks) {
			c.status = 2
			log.Printf("All reduce tasks done! Coordinator is finishing.")
		}
	}
	return nil
}

func (c *Coordinator) RegisterWorker(n *None, workerID *int) error {
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
