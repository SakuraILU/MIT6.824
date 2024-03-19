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
	"strconv"
	"strings"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := &ExampleArgs{}
		job := &Job{}
		if call("Master.GetJob", args, job) == false {
			time.Sleep(1 * time.Second)
			continue
		}

		switch job.Type {
		case MAP:
			DoMapf(mapf, job)
		case REDUCE:
			DoReducef(reducef, job)
		case DONE:
		}

		reply := &ExampleReply{}
		if call("Master.JobDone", job, reply) == false {
			// log.Printf("job %v may be already finished by others", job.Id)
			time.Sleep(1 * time.Second)
			continue
		}

		if job.Type == DONE {
			return
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func DoMapf(mapf func(string, string) []KeyValue, job *Job) {
	kvas := make([]KeyValue, 0)
	for _, filename := range job.Filename {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kvas = append(kvas, mapf(filename, string(content))...)
	}

	intermediates := make([][]KeyValue, job.NReduce)

	for _, kva := range kvas {
		idx := ihash(kva.Key) % job.NReduce
		intermediates[idx] = append(intermediates[idx], kva)
	}

	for idx, intermediate := range intermediates {
		fname := "intermediate" + strconv.Itoa(int(job.Id)) + "-" + strconv.Itoa(idx)
		of, err := os.Create(fname)
		if err != nil {
			panic(err)
		}
		// json format...
		encoder := json.NewEncoder(of)
		encoder.Encode(intermediate)
		of.Close()
	}
}

func DoReducef(reducef func(string, []string) string, job *Job) {
	intermediate := []KeyValue{}
	for _, filename := range job.Filename {
		f, err := os.Open(filename)
		if err != nil {
			panic("file is not exist")
		}
		dec := json.NewDecoder(f)

		kvas := []KeyValue{}
		if err := dec.Decode(&kvas); err != nil {
			panic(fmt.Sprintf("decode error for file %s", filename))
		}

		intermediate = append(intermediate, kvas...)

		f.Close()
	}

	sort.Sort(ByKey(intermediate))

	strs := strings.Split(job.Filename[0], "-")
	if len(strs) != 2 {
		panic(fmt.Sprintf("invalid file %s", job.Filename))
	}
	oname := "mr-out-" + strs[1]

	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
