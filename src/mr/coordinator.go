package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Queue struct {
	files []string
	m     sync.Mutex
}

func (q *Queue) Push(value string) {
	q.m.Lock()
	q.files = append(q.files, value)
	q.m.Unlock()
}

func (q *Queue) Pushs(values []string) {
	q.m.Lock()
	q.files = append(q.files, values...)
	q.m.Unlock()
}

func (q *Queue) Pop() string {
	q.m.Lock()
	ret := q.files[0]
	q.files = q.files[1:]
	q.m.Unlock()
	return ret
}

func (q *Queue) Empty() bool {
	q.m.Lock()
	ret := len(q.files) == 0
	q.m.Unlock()
	return ret
}

type Status struct {
	workerID  int
	timestamp int64
	idx       int
	files     []string
}

type Coordinator struct {
	// Your definitions here.
	workerUniqueID int
	nReduce        int

	mapTaskNum    int
	reduceTaskNum []int

	mapTaskStatus map[int]Status
	mapTaskFinish bool

	reduceTaskStatus map[int]Status
	reduceTaskFinish bool

	inputFiles        Queue
	interMediateFiles [][]string

	workers       map[int]bool
	activeWorkers map[int]bool

	completed bool

	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Init(files []string, nReduce int) {
	c.workerUniqueID = 0
	c.nReduce = nReduce

	c.mapTaskNum = 0
	c.reduceTaskNum = []int{}
	for i := 0; i < nReduce; i++ {
		c.reduceTaskNum = append(c.reduceTaskNum, i)
	}

	c.mapTaskStatus = make(map[int]Status)
	c.mapTaskFinish = false

	c.reduceTaskStatus = make(map[int]Status)
	c.reduceTaskFinish = false

	c.inputFiles.Pushs(files)
	c.interMediateFiles = [][]string{}
	for i := 0; i < nReduce; i++ {
		c.interMediateFiles = append(c.interMediateFiles, []string{})
	}

	c.workers = make(map[int]bool)
	c.activeWorkers = make(map[int]bool)

	c.completed = false
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.mutex.Lock()
	reply.WorkerID = c.workerUniqueID
	c.workers[reply.WorkerID] = true
	c.activeWorkers[reply.WorkerID] = true
	c.workerUniqueID++
	c.mutex.Unlock()
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	id := args.WorkerID
	c.mutex.Lock()
	// worker die
	if _, ok := c.workers[id]; !ok {
		reply.TaskType = Done
		c.mutex.Unlock()
		return nil
	}
	if !c.inputFiles.Empty() {
		// map task
		c.mapTaskNum++
		taskNum := c.mapTaskNum
		file := c.inputFiles.Pop()

		reply.TaskType = Map
		reply.Files = append(reply.Files, file)
		reply.TaskNum = taskNum

		status := Status{}
		status.workerID = id
		status.timestamp = time.Now().Unix()
		status.files = append(status.files, file)
		c.mapTaskStatus[taskNum] = status
		log.Printf("task %v timestamp: %v, file: %v", taskNum, status.timestamp, file)
	} else if !c.mapTaskFinish {
		// wait
		reply.TaskType = Wait
	} else if len(c.reduceTaskNum) != 0 {
		// reduce task
		taskNum := c.reduceTaskNum[0]
		c.reduceTaskNum = c.reduceTaskNum[1:]
		files := c.interMediateFiles[taskNum]
		c.interMediateFiles[taskNum] = []string{}

		reply.TaskType = Reduce
		reply.Files = files
		reply.TaskNum = taskNum

		status := Status{}
		status.workerID = id
		status.timestamp = time.Now().Unix()
		status.idx = taskNum
		status.files = files
		c.reduceTaskStatus[taskNum] = status
		log.Printf("task timestamp: %v", status.timestamp)
	} else if !c.reduceTaskFinish {
		// wait
		reply.TaskType = Wait
	} else if c.reduceTaskFinish {
		reply.TaskType = Done
		delete(c.workers, id)
		if c.workerUniqueID > 0 && len(c.workers) == 0 {
			c.completed = true
		}
	} else {
		log.Fatalf("should not run here")
	}
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) CheckTimeOut() {
	// check map task
	for !c.mapTaskFinish {
		ts := time.Now().Unix()
		c.mutex.Lock()
		deleteTask := []int{}
		for taskNum, status := range c.mapTaskStatus {
			if ts-status.timestamp > 10 {
				// 标记 delete 的 task
				deleteTask = append(deleteTask, taskNum)
				// 回收 inputfiles
				c.inputFiles.Pushs(status.files)
				// 删除 worker
				delete(c.workers, status.workerID)
			}
		}
		for _, taskNum := range deleteTask {
			delete(c.mapTaskStatus, taskNum)
		}
		c.mutex.Unlock()
		time.Sleep(time.Second)
	}

	for !c.reduceTaskFinish {
		ts := time.Now().Unix()
		c.mutex.Lock()
		deleteTask := []int{}
		for taskNum, status := range c.reduceTaskStatus {
			if ts-status.timestamp > 10 {
				// 标记 delete 的 task
				deleteTask = append(deleteTask, taskNum)
				// 回收 intermediatefiles
				c.interMediateFiles[status.idx] = append(c.interMediateFiles[status.idx], status.files...)
				// 删除 worker
				delete(c.workers, status.workerID)
			}
		}
		for _, taskNum := range deleteTask {
			delete(c.reduceTaskStatus, taskNum)
			c.reduceTaskNum = append(c.reduceTaskNum, taskNum)
		}
		c.mutex.Unlock()
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) ReportTaskFinish(args *ReportArgs, reply *ReportReply) error {
	id := args.WorkerID
	taskNum := args.TaskNum
	ts := time.Now().Unix()
	log.Printf("report ts: %v", ts)
	switch args.TaskType {
	case Map:
		c.mutex.Lock()
		// 检查 Coordinator 视角中 worker 是否存活
		if _, ok := c.workers[id]; !ok {
			log.Printf("worker %v exit", id)
			reply.Ok = false
			c.mutex.Unlock()
			return nil
		}

		// task 是否过期
		if _, ok := c.mapTaskStatus[taskNum]; !ok {
			log.Printf("task: %v error", taskNum)
			reply.Ok = false
			c.mutex.Unlock()
			return nil
		}

		status := c.mapTaskStatus[taskNum]

		if ts-status.timestamp > 10 {
			reply.Ok = false
			log.Printf("task: %v error", taskNum)
			c.inputFiles.Pushs(status.files)
			delete(c.workers, status.workerID)
			delete(c.mapTaskStatus, taskNum)
			c.mutex.Unlock()
			return nil
		}

		// 确认 task 完成
		delete(c.mapTaskStatus, taskNum)
		// 存进 intermediatefiles
		for _, file := range args.Files {
			idx := int(file[len(file)-1] - '0')
			c.interMediateFiles[idx] = append(c.interMediateFiles[idx], file)
		}

		// check whether all map task are finished
		if len(c.mapTaskStatus) == 0 && c.inputFiles.Empty() {
			c.mapTaskFinish = true
		}
		c.mutex.Unlock()
	case Reduce:
		c.mutex.Lock()
		// 检查 Coordinator 视角中 worker 是否存活
		if _, ok := c.workers[id]; !ok {
			reply.Ok = false
			c.mutex.Unlock()
			return nil
		}

		// task 是否过期
		if _, ok := c.reduceTaskStatus[taskNum]; !ok {
			reply.Ok = false
			c.mutex.Unlock()
			return nil
		}
		status := c.reduceTaskStatus[taskNum]
		if ts-status.timestamp > 10 {
			reply.Ok = false
			c.interMediateFiles[status.idx] = append(c.interMediateFiles[status.idx], status.files...)
			delete(c.workers, status.workerID)
			delete(c.reduceTaskStatus, taskNum)
			c.reduceTaskNum = append(c.reduceTaskNum, taskNum)
			c.mutex.Unlock()
			return nil
		}

		// 确认 task 完成
		delete(c.reduceTaskStatus, taskNum)
		// check whether all reduce tasks are finished
		if len(c.reduceTaskStatus) == 0 && len(c.reduceTaskNum) == 0 {
			c.reduceTaskFinish = true
		}
		c.mutex.Unlock()
	}

	reply.Ok = true
	return nil
}

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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := c.completed

	// Your code here.
	if c.completed {
		log.Println("Coordinator exit")
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Init(files, nReduce)

	// 启动协程监听 task status
	go c.CheckTimeOut()

	c.server()
	return &c
}
