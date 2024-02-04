package mr

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
		
	initiate_worker()
	fmt.Print("< worker started\n")
	defer fmt.Print("< worker exited\n")
	
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	
	exit_flag := false
	// check "please exit"
	for call("Coordinator.CheckExitFlag", 0, &exit_flag) && !exit_flag { 
		reply := RequestJobReply{}
		if !call("Coordinator.RequestJob", 0, &reply) {
			fmt.Print("< requestJob failed\n") 
			// break // bad, cause maybe some map tasks lag, but reduce hasn't started. cannot break.
			time.Sleep(time.Duration(sleep_interval) * time.Millisecond)
		}

		switch task := reply.Task.(type) {
		case MapTask:
			fmt.Printf("< map job %v accepted\n", task.Map_task_id)
			
			err := run_mapf(mapf, task)
			if err != nil {
				fmt.Print("< " + err.Error(), "\n")
				return
			}

			fmt.Printf("> map job %v finished\n", task.Map_task_id)
		case ReduceTask:
			fmt.Printf("> reduce job %v accepted\n", task.Reduce_task_id)

			err := run_reducef(reducef, task)
			if err != nil {
				fmt.Print("< " + err.Error(), "\n")
				return
			}

			fmt.Printf("< reduce job %v finished\n", task.Reduce_task_id)

		}
	}
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	fmt.Println("< " + err.Error() + "\n")
	return false
}




// My varibles

// My functions


func initiate_worker() {
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
}

func key_value_write_to_file(filename string, kvlist []KeyValue) error {
	filename_alter := filename + ".tmp" // temporary file, atomically renamed afterwards.
	
	file, err := os.Create(filename_alter) 
	if err != nil {
		log.Fatalf("cannot create %v", filename_alter)
		return err
	}

	// enc := json.NewEncoder(file)
	// for _, kv	:= range kvlist {
	// 	err := enc.Encode(&kv)
	// 	if err != nil {
	// 		log.Fatalf("cannot encode when saving to", filename)
	// 		return err
	// 	}
	// }

	for _, kv	:= range kvlist {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}

	file.Close()

	// rename
	os.Rename(filename_alter, filename)

	return nil
}

// by append
func key_value_read_from_file(filename string, kvlist *[]KeyValue) error {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return err
	}

	// dec := json.NewDecoder(file)
	// for	true {
	// 	var kv KeyValue
	// 	if err := dec.Decode(&kv); err != nil {
  //     break
  //   }
	// 	*kvlist = append(*kvlist, kv)
	// }

	for true {
		var key, value string
		read_n, err := fmt.Fscanf(file, "%s %s", &key, &value)
		if err != nil || read_n <= 0 {
			break
		}
		*kvlist = append(*kvlist, KeyValue{key, value})
	}
	
	// fmt.Printf("KVRFT KVLIST SIZE %v \n", len(*kvlist))

	file.Close()
	return nil
}

func reduce_write_result_to_file(filename string, result []KeyValue) error {
	filename_alter := filename + ".tmp" // temporary file, atomically renamed afterwards.
	
	file, err := os.Create(filename_alter) 
	if err != nil {
		log.Fatalf("cannot create %v", filename_alter)
		return err
	}
	
	for _, kv := range result {
		key := kv.Key
		value := kv.Value
		fmt.Fprintf(file, "%v %v\n", key, value)
	}

	file.Close()

	// rename
	os.Rename(filename_alter, filename)

	return nil
}

// RUN MAP FUNCTION
func run_mapf(mapf func(string, string) []KeyValue, task MapTask) error {
	filename := task.Filename
	map_task_id := task.Map_task_id
	nReduce := task.NReduce

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return Error{"cannot open " + filename}
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return Error{"cannot read " + filename}
	}
	file.Close()
	
	kvlist := mapf(filename, string(content))
	kvlist_sub := make([]([]KeyValue), nReduce)
	
	for _, kv := range kvlist {
		i := ihash(kv.Key) % nReduce
		kvlist_sub[i] = append(kvlist_sub[i], kv)
	}

	args1 := QueryJobStatusArgs{false, map_task_id}
	result1 := 0
	if !call("Coordinator.QueryJobStatus", &args1, &result1) {
		return Error{"QueryJobStatus failed"}
	}

	if result1 == 1 {
		fmt.Print("< other machine has already done the job\n")
		return nil
	}

	for i := 0; i < nReduce; i++ {
		filename := intermediate_file_prefix + fmt.Sprint(map_task_id) + "-" + fmt.Sprint(i)
		err := key_value_write_to_file(filename, kvlist_sub[i])
		if err != nil {
			log.Fatalf("cannot write intermediate file %v", filename)
			return Error{"cannot write intermediate file " + filename}
		}
	}

	// if !call("Coordinator.UpdateJobStatus"
		// log.Fatalf("cannot update job status")
		// return 

	args2 := UpdateJobStatusArgs{false, map_task_id, 1}
	result2 := 0
	if !call("Coordinator.UpdateJobStatus", &args2, &result2) {
		return Error{"UpdateJobStatus failed"}
	}
	
	return nil
}

// RUN REDUCE FUNCTION

func run_reducef(reducef func(string, []string) string, task ReduceTask) error {

	reduce_task_id := task.Reduce_task_id
	nMap := task.NMap

	kvlist := []KeyValue{}
	for i := 0; i < nMap; i++ {
		filename := intermediate_file_prefix + fmt.Sprint(i) + "-" + fmt.Sprint(reduce_task_id)
		err := key_value_read_from_file(filename, &kvlist)
		if err != nil {
			fmt.Print("< "+err.Error())
			return err
		}
	}

	values_buckets := map[string]([]string){}

	for _, kv := range kvlist {
		key := kv.Key
		value := kv.Value
		values_buckets[key] = append(values_buckets[key], value)
	}

	outputs := []KeyValue{}
	for key, values := range values_buckets {
		output := reducef(key, values)
		outputs = append(outputs, KeyValue{key, output})
	}

	args1 := QueryJobStatusArgs{true, reduce_task_id}
	result1 := 0
	if !call("Coordinator.QueryJobStatus", &args1, &result1) {
		return Error{"QueryJobStatus failed"}
	}

	if result1 == 1 {
		fmt.Print("< other machine has already done the job\n")
		return nil
	}

	filename := output_file_prefix + fmt.Sprint(reduce_task_id)
	err := reduce_write_result_to_file(filename, outputs)
	if err != nil {
		log.Fatalf("cannot write intermediate file %v", filename)
		return Error{"cannot write output file " + filename}
	}

	args2 := UpdateJobStatusArgs{true, reduce_task_id, 1}
	result2 := 0
	if !call("Coordinator.UpdateJobStatus", &args2, &result2) {
		return Error{"UpdateJobStatus failed"}
	}
	
	return nil
}
