package mr

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var intermediate_file_prefix string = "mr-" // mr-map_task_id-reduce_task_id

var output_file_prefix string = "mr-out-" // mr-out-reduce_task_id

var timeout_interval int = 10e9 // unit 1e-9s default 10s

var sleep_interval int = 50 // unit ms

type MapTask struct {
	Filename string
	Map_task_id int
	NReduce int
}

type ReduceTask struct {
	Reduce_task_id int
	NMap int
}

type Coordinator struct {
	// Your definitions here.

	is_map_finished bool
	is_reduce_finished bool // also used as global exit_flag
	
	nMap int
	nReduce int
	
	input_files []string
	
	map_job_status []int // -1 unassigned, 0 assigned yet finished, 1 finished
	map_job_start_time []time.Time // to detect timeout
	
	reduce_job_status []int
	reduce_job_start_time []time.Time
	
	modify_lock sync.Mutex
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
	
	// add registration info to gob
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
	
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

	fmt.Printf("> coordinator server started\n")
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// ret := true
	c.lock()
	defer c.unlock()
	// Your code here.
	return c.is_reduce_finished // c.supervisor() will be responsible for updating exit_flag.

	// return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	
	// Your code here.

	c.is_map_finished = false
	c.is_reduce_finished = false
	c.nMap = len(files)
	c.nReduce = nReduce
	c.input_files = files

	c.map_job_status = make([]int, c.nMap)
	for i, _ := range c.map_job_status {
		c.map_job_status[i] = -1
	}

	c.map_job_start_time = make([]time.Time, c.nMap)

	c.reduce_job_status = make([]int, c.nReduce)
	for i, _ := range c.reduce_job_status {
		c.reduce_job_status[i] = -1
	}
	c.reduce_job_start_time = make([]time.Time, c.nReduce)

	go c.supervisor()

	c.server()
	return &c
}

// My functions

// RPCS
// respond to RequestJob
func (c *Coordinator) RequestJob(_ int, reply *RequestJobReply) error {
	c.lock()
	defer c.unlock()

	if !c.is_map_finished {
		for i := 0; i < c.nMap; i++ {
			if (c.map_job_status[i] == -1) {
				c.map_job_status[i] = 0
				c.map_job_start_time[i] = time.Now()
				reply.Task = MapTask{c.input_files[i], i, c.nReduce}
				fmt.Printf("> map job %v delivered\n", i)
				return nil
			}
		}
	} else if !c.is_reduce_finished {
		for i := 0; i < c.nReduce; i++ {
			if (c.reduce_job_status[i] == -1) {
				c.reduce_job_status[i] = 0
				c.reduce_job_start_time[i] = time.Now()
				reply.Task = ReduceTask{i, c.nMap}
				fmt.Printf("> reduce job %v delivered\n", i)
				return nil
			}
		}
	}
	// return Error{"no more jobs"}
	return nil
}

// respond to exit_flag request
func (c *Coordinator) CheckExitFlag(_ int, result *bool) error {
	*result = c.Done()
	return nil
}

// query job status
func (c *Coordinator) QueryJobStatus(args *QueryJobStatusArgs, result *int) error {
	// fmt.Printf("QueryJobStatus called by %v\n", args.TaskId)
	c.lock()
	defer c.unlock()
	if !args.TaskType { // Map
		*result = c.map_job_status[args.TaskId]
	} else {
		*result = c.reduce_job_status[args.TaskId]
	}

	// fmt.Printf("REQ status %v, result %v\n", args.TaskId, *result)
	return nil
}

// update job status
// result = previous status
func (c *Coordinator) UpdateJobStatus(args *UpdateJobStatusArgs, result *int) error {
	c.lock()
	defer c.unlock()

	// fmt.Printf("UpdateJobStatus called by %v\n", args.TaskId)

	if !args.TaskType {
		*result = c.map_job_status[args.TaskId]
		c.map_job_status[args.TaskId] = args.Status

		fmt.Printf("> map job %v finished\n", args.TaskId)
	} else {
		*result = c.reduce_job_status[args.TaskId]
		c.reduce_job_status[args.TaskId] = args.Status
		
		fmt.Printf("> reduce job %v finished\n", args.TaskId)
	}
	return nil
}

// Local

func (c *Coordinator) lock() {
	c.modify_lock.Lock()
}

func (c *Coordinator) unlock() {
	c.modify_lock.Unlock()
}

func (c *Coordinator) supervisor() {
	fmt.Printf("> coordinator supervisor started\n")

	for { // Map
		{
			c.lock()
			if (c.is_map_finished) {
				c.unlock()
				break
			}
			// fmt.Printf("> supervisor started a new round\n")

			finished := true
			for i := 0; i < c.nMap; i++ {
				finished = finished && c.map_job_status[i] == 1
				if c.map_job_status[i] == 0 {
					time_gap := time.Since(c.map_job_start_time[i])
					if int(time_gap) > timeout_interval {
						fmt.Printf("> map job %v timeout, reset after %v\n", i, time_gap)
						c.map_job_status[i] = -1
					}
				}
			}

			c.is_map_finished = finished
			c.unlock()
		}
		time.Sleep(time.Duration(sleep_interval) * time.Millisecond)
	}

	for {
		{
			c.lock()
			if (c.is_reduce_finished) {
				c.unlock()
				break
			}
			// fmt.Printf("> supervisor started a new round")
			finished := true
			for i := 0; i < c.nReduce; i++ {
				finished = finished && c.reduce_job_status[i] == 1
				if c.reduce_job_status[i] == 0 {
					time_gap := time.Since(c.reduce_job_start_time[i])
					if int(time_gap) > timeout_interval {
						fmt.Printf("> reduce job %v timeout, reset after %v\n", i, time_gap)
						c.reduce_job_status[i] = -1
					}
				}
			}

			c.is_reduce_finished = finished
			c.unlock()
		}
		time.Sleep(time.Duration(sleep_interval) * time.Millisecond)
	}
	fmt.Print("> coordinator supervisor exited\n")
}