package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	State             int
	MapChan           chan *Task
	MapTasks          []*Task
	ReduceChan        chan *Task
	ReduceTasks       []*Task
	RemainMapTasks    int
	RemainReduceTasks int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

func (c *Coordinator) GetTask(args *ExampleArgs, reply *Task) error {
	mutex.Lock()
	defer mutex.Unlock()
	if c.State == Map {
		if len(c.MapChan) > 0 {
			*reply = *<-c.MapChan
			c.MapTasks[reply.TaskId].Start = time.Now()
		} else {
			for i := 0; i < len(c.MapTasks); i++ {
				if !c.MapTasks[i].Finished && time.Since(c.MapTasks[i].Start) > time.Second*15 {
					*reply = *c.MapTasks[i]
					c.MapTasks[i].Start = time.Now()
					break
				}
			}
		}
	} else if c.State == Reduce {
		if len(c.ReduceChan) > 0 {
			*reply = *<-c.ReduceChan
			c.ReduceTasks[reply.TaskId].Start = time.Now()
		} else {
			for i := 0; i < len(c.ReduceTasks); i++ {
				if !c.ReduceTasks[i].Finished && time.Since(c.ReduceTasks[i].Start) > time.Second*15 {
					*reply = *c.ReduceTasks[i]
					c.ReduceTasks[i].Start = time.Now()
					break
				}
			}
		}
	} else {
		reply.TaskType = JobDone
	}
	return nil
}

func (c *Coordinator) UpdateTask(task *Task, reply *ExampleReply) error {
	mutex.Lock()
	defer mutex.Unlock()
	if task.TaskType == Map {
		if !c.MapTasks[task.TaskId].Finished {
			c.MapTasks[task.TaskId].Finished = true
			c.RemainMapTasks--
		}
	} else if task.TaskType == Reduce {
		if !c.ReduceTasks[task.TaskId].Finished {
			c.ReduceTasks[task.TaskId].Finished = true
			c.RemainReduceTasks--
		}
	}
	// Update c state
	if c.RemainReduceTasks == 0 {
		c.State = Waiting
	} else if c.RemainMapTasks == 0 && c.RemainReduceTasks != 0 {
		c.State = Reduce
	}
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
	// Your code here.
	ret := (c.State == Waiting)

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:             Map,
		MapChan:           make(chan *Task, len(files)),
		ReduceChan:        make(chan *Task, nReduce),
		MapTasks:          make([]*Task, len(files)),
		ReduceTasks:       make([]*Task, nReduce),
		RemainMapTasks:    len(files),
		RemainReduceTasks: nReduce,
	}
	// Create map tasks.
	i := 0
	for _, file := range files {
		task := Task{
			TaskType: Map,
			Filename: file,
			NReduce:  nReduce,
			TaskId:   i,
			Finished: false,
			Start:    time.Now(),
		}
		c.MapChan <- &task
		c.MapTasks[i] = &task
		i++
	}
	// Create reduce tasks.
	for i := 0; i < nReduce; i++ {
		task := Task{
			TaskType: Reduce,
			Filename: "mr-",
			NReduce:  len(c.MapTasks),
			TaskId:   i,
			Finished: false,
			Start:    time.Now(),
		}
		c.ReduceChan <- &task
		c.ReduceTasks[i] = &task
	}

	c.server()
	return &c
}
