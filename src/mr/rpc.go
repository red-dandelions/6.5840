package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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
type RegisterArgs struct {
}
type RegisterReply struct {
	WorkerID int
	NReduce  int
}

type Type int

const (
	Map Type = iota
	Reduce
	Done
	Wait
)

type TaskArgs struct {
	WorkerID int
}
type TaskReply struct {
	TaskType Type
	Files    []string
	TaskNum  int
}

type ReportArgs struct {
	WorkerID int
	TaskType Type
	TaskNum  int
	Files    []string
}
type ReportReply struct {
	Ok bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
