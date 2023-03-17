package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	info := Register()
	id := info.WorkerID
	nReduce := info.NReduce
	for {
		task := GetTask(id)

		switch task.TaskType {
		case Map:
			result := MapTask(id, nReduce, task, mapf)
			ok := ReportTaskFinish(id, task, result)
			if !ok {
				log.Println("map report not ok")
				return
			}
		case Reduce:
			log.Printf("do reduce: %v\n", task.TaskNum)
			ReduceTask(task, reducef)
			ok := ReportTaskFinish(id, task, nil)
			if !ok {
				log.Println("reduce report not ok")
				return
			}
		case Wait:
			log.Println("wait for 2 s")
			time.Sleep(2 * time.Second)
		case Done:
			log.Println("task type Done")
			return
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

	fmt.Println(err)
	return false
}

func Register() RegisterReply {
	args := RegisterArgs{}
	info := RegisterReply{}
	ok := call("Coordinator.Register", &args, &info)
	if !ok {
		log.Fatalln("call rpc Register failed")
	}
	return info
}

func GetTask(id int) TaskReply {
	req := TaskArgs{}
	req.WorkerID = id
	task := TaskReply{}
	ok := call("Coordinator.GetTask", &req, &task)
	if !ok {
		log.Fatalln("call rpc GetTask failed")
	}
	return task
}

func MapTask(id int, nReduce int, task TaskReply, mapf func(string, string) []KeyValue) []string {
	log.Printf("worker: %v, do map file: %v", id, task.Files)
	tp0 := time.Now().Unix()

	log.Printf("id: %v map", id)
	kva := []KeyValue{}
	for _, file := range task.Files {
		ofile, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		content, err := ioutil.ReadAll(ofile)
		if err != nil {
			log.Fatalf("cannnot read %v", file)
		}
		ofile.Close()

		kva = append(kva, mapf(file, string(content))...)
	}

	log.Printf("id: %v , write file", id)
	result := []string{}
	ofiles := []*os.File{}
	for i := 0; i < nReduce; i++ {
		result = append(result, fmt.Sprintf("mr-%v-%v", task.TaskNum, i))
		ofile, _ := os.Create(result[i])
		ofiles = append(ofiles, ofile)
	}
	encs := []*json.Encoder{}
	for _, o := range ofiles {
		encs = append(encs, json.NewEncoder(o))
	}

	sort.Sort(ByKey(kva))
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		err := encs[idx].Encode(&kv)
		if err != nil {
			log.Fatalf("encode error: %v", err)
		}
	}
	for _, o := range ofiles {
		o.Close()
	}
	log.Printf("id: %v, end write", id)
	tp1 := time.Now().Unix()
	log.Printf("file: %v, kva size: %v, cost: %v s\n", task.Files, len(kva), tp1-tp0)
	return result
}

func ReportTaskFinish(id int, task TaskReply, intermediatefile []string) bool {
	req := ReportArgs{}
	req.WorkerID = id
	req.TaskType = task.TaskType
	req.TaskNum = task.TaskNum
	if intermediatefile != nil {
		req.Files = intermediatefile
	}

	ack := ReportReply{}
	ok := call("Coordinator.ReportTaskFinish", &req, &ack)
	if !ok {
		log.Fatalf("call rpc ReportTaskFinish failed")
	}
	return ack.Ok
}

func ReduceTask(task TaskReply, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for _, file := range task.Files {
		ofile, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		dec := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%v", task.TaskNum)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
}
