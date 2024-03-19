package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
)

type Phase int

const (
	MAPPHASE Phase = iota
	REDUCEPHASE
	DONEPAHSE
)

type Master struct {
	// Your definitions here.
	jm      *JobManager
	nmap    int
	nreduce int

	phase Phase
}

func NewMaster(files []string, nreduce int) (m *Master) {
	nmap := len(files)

	m = &Master{
		jm:      NewJobManager(nmap + nreduce),
		nmap:    nmap,
		nreduce: nreduce,
		phase:   MAPPHASE,
	}

	m.MakeMapJobs(files)

	return
}

func (m *Master) MakeMapJobs(files []string) {
	for _, file := range files {
		job := NewJob(MAPREADY, []string{file}, m.nreduce)
		m.jm.Push(job)
	}
}

func (m *Master) MakeReduceJobs() {
	log.Println("MakeReduceJobs")
	intermediates := make([][]string, m.nreduce)
	entries, err := ioutil.ReadDir(".")
	if err != nil {
		panic("open current dir failed")
	}

	for _, entry := range entries {
		fname := entry.Name()
		log.Printf("fname: %s", fname)
		if strings.HasPrefix(fname, "intermediate") {
			idx, err := strconv.Atoi(fname[len(fname)-1:])
			if err != nil {
				panic("error intermediate name")
			}
			intermediates[idx] = append(intermediates[idx], fname)
		}
	}

	for _, intermediate := range intermediates {
		job := NewJob(REDUCEREADY, intermediate, m.nreduce)
		m.jm.Push(job)
	}
}

func (m *Master) GetJob(args *ExampleArgs, job *Job) (err error) {
	j := m.jm.Pop()
	if j == nil {
		if m.phase == DONEPAHSE {
			j = NewJob(DONE, nil, 0)
		} else {
			err = fmt.Errorf("no job is to be disturbed")
			return
		}
	}

	*job = *j
	return nil
}

func (m *Master) JobDone(job *Job, reply *ExampleReply) (err error) {
	if m.phase == DONEPAHSE {
		return
	}

	if err = m.jm.JobDone(job); err != nil {
		return
	}

	if m.jm.CheckAllDone() {
		switch m.phase {
		case MAPPHASE:
			m.MakeReduceJobs()
			m.phase = REDUCEPHASE
		case REDUCEPHASE:
			m.phase = DONEPAHSE
		}
	}

	return
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	// Your code here.

	return m.phase == DONEPAHSE
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := NewMaster(files, nReduce)
	// Your code here.

	m.server()
	return m
}
