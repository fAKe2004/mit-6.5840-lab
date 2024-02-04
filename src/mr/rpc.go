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

type Error struct {
	err_msg string
}

func (e Error) Error() string {
	return e.err_msg
}



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

// RequestJob
// worker to coordinator
type RequestJobReply struct {
	Task interface{}
}

type QueryJobStatusArgs struct {
	TaskType bool // 0 for Map, 1 for Reduce
	TaskId int
}

type UpdateJobStatusArgs struct {
	TaskType bool // 0 for Map, 1 for Reduce
	TaskId int
	Status int
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

