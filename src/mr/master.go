package mr

import (
	"encoding/gob"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Master struct {
	NReduce        int
	MapBacklog     []Reply
	ReducerBacklog []Reply
	IsProcessDone  bool
	Lock           sync.Mutex
	NFilesLeft     int
	NReducLeft     int
	// DoneFiles        map[string]bool
	CountedDoneFiles map[string]bool
}

// RPC handlers for the worker to call.
func (m *Master) checkWorker(reply Reply) {
	time.Sleep(time.Second * 10)
	// fmt.Printf("checking task: %v\n", reply.FileName)
	m.Lock.Lock()
	defer m.Lock.Unlock()
	var isDone bool
	if reply.TT == mapTask {
		isDone = m.CountedDoneFiles[reply.FileName]
		if !isDone {
			m.MapBacklog = append(m.MapBacklog, reply)
		}
	}
	if reply.TT == reduceTask {
		isDone = m.CountedDoneFiles[strconv.Itoa(reply.FileIdx)]
		if !isDone {
			m.ReducerBacklog = append(m.ReducerBacklog, reply)
		}
	}

}

func (m *Master) DoneTask(task *Task, reply *Reply) error {
	// fmt.Printf("done task: %v\n", task.FileName)

	m.Lock.Lock()
	defer m.Lock.Unlock()
	// m.DoneFiles[task.FileName] = true
	if !m.CountedDoneFiles[task.FileName] {
		// fmt.Printf("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa: %v\n", task.FileName)
		// fmt.Println(m.CountedDoneFiles[task.FileName])
		// fmt.Println(m.NReducLeft)
		if task.TT == mapTask {
			if m.NFilesLeft > 0 {
				m.NFilesLeft--
			}
		}
		if task.TT == reduceTask {
			if m.NReducLeft > 0 {
				m.NReducLeft--
			}
		}
	}

	m.CountedDoneFiles[task.FileName] = true
	return nil
}

func (m *Master) GetNextTask(task *Task, reply *Reply) error {

	m.Lock.Lock()
	defer m.Lock.Unlock()
	if len(m.MapBacklog) > 0 {
		// fmt.Printf("choosing mapper task from backlog\n")
		poppedTask := m.MapBacklog[0]
		m.MapBacklog = m.MapBacklog[1:]
		// fmt.Printf("popped m: %v\n", poppedTask.FileName)

		*reply = poppedTask
		reply.Success = true
		go m.checkWorker(*reply)

	} else {
		if m.NFilesLeft == 0 && len(m.ReducerBacklog) > 0 {

			if m.NReducLeft == 0 {
				// fmt.Printf("oops. no more tasks in backlog\n")
				// m.IsProcessDone = true
			} else {
				// fmt.Printf("choosing reducer task from backlog\n")
				poppedTask := m.ReducerBacklog[0]
				m.ReducerBacklog = m.ReducerBacklog[1:]
				// fmt.Printf("popped r: %v\n", poppedTask.FileIdx)

				*reply = poppedTask
				reply.Success = true
				go m.checkWorker(*reply)
			}
		} else {
			// if m.NReducLeft > 0 {
			reply.Success = true
			reply.TT = idle
			// }
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.Lock.Lock()
	status := (m.NFilesLeft == 0) && (m.NReducLeft == 0)
	m.Lock.Unlock()
	return status
}

//
// create a Master.
// main/mrmaster.go calls this function.// Your worker implementation here.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	gob.Register(Task{})
	gob.Register(Reply{})

	m.server()
	m.NReduce = nReduce
	m.IsProcessDone = false
	for i, file := range files {
		newTask := Reply{TT: mapTask, FileName: file, IsDone: false, FileIdx: i, NReducers: nReduce}
		m.MapBacklog = append(m.MapBacklog, newTask)
	}

	// m.Lock = sync.Mutex{}

	m.NFilesLeft = len(m.MapBacklog)
	m.NReducLeft = nReduce
	// m.DoneFiles = make(map[string]bool)
	m.CountedDoneFiles = make(map[string]bool)

	for i := 0; i < nReduce; i++ {
		newTask := Reply{TT: reduceTask, FileIdx: i, IsDone: false}
		m.ReducerBacklog = append(m.ReducerBacklog, newTask)
	}

	return &m
}
