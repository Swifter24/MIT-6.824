package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type MrTaskArgs struct {
	WorkerID int
}

type MrTaskReply struct {
	//任务类型 1 map任务 2 reduce任务
	TaskType int
	//Reduce任务数量
	ReduceCount int
	//Map任务文件名
	MapName string
	//Map任务ID
	MapID int
	//Reduce任务ID
	ReduceID int
	//Map任务数量
	MapTaskCount int
	//是否退出Worker
	Exit bool
}
type TaskDoneArgs struct {
	TaskType int
	MapName  string
	ReduceID int
}
type WorkerExitArgs struct {
	WorkerID int
}
type None struct{}

var intermediateFileName = "mr-%v-%v"
var outputFileName = "mr-out-%v"

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
