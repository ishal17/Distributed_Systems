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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

func getTempFileName(oname string, middleFiles *[]KeyValue) string {
	ans := ""
	for _, kv := range *middleFiles {
		if kv.Value == oname {
			return kv.Key
		}
	}
	return ans
}

func doMapTask(nRed int, filename string, mapf func(string, string) []KeyValue, idx int) {
	// fmt.Printf("start mapping\n")

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	sort.Sort(ByKey(kva))
	var middleFiles []KeyValue
	for i := 0; i < nRed; i++ {
		oname := "mr-" + strconv.Itoa(idx) + "-" + strconv.Itoa(i)
		tempFile, err := ioutil.TempFile(".", oname)
		if err != nil {
			// fmt.Printf("error in creating temporary file\n")
		}
		tempFile.Close()
		middleFiles = append(middleFiles, KeyValue{tempFile.Name(), oname})
	}

	// fmt.Println(middleFiles)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		oname := "mr-" + strconv.Itoa(idx) + "-" + strconv.Itoa(ihash(kva[i].Key)%nRed)
		tmpname := getTempFileName(oname, &middleFiles)
		file, err := os.OpenFile(tmpname, os.O_APPEND|os.O_RDWR, os.ModePerm)
		if err != nil {
			log.Fatalf("error %v", err)
		}
		enc := json.NewEncoder(file)

		if enc == nil {
			log.Fatalf("error creating encoder")
		}

		for k := i; k < j; k++ {
			err := enc.Encode(&kva[k])
			if err != nil {
				fmt.Println(kva[k])
				log.Fatalf("error %v", err)
			}
		}
		file.Close()
		i = j
	}

	for _, kv := range middleFiles {
		os.Rename(kv.Key, kv.Value)
	}
}

func doReduceTask(reducef func(string, []string) string, idx int) {
	// fmt.Printf("start reducing\n")
	// fmt.Printf("reducing task: %v\n", idx)

	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}
	oname := "mr-out-" + strconv.Itoa(idx)
	tempFile, err := ioutil.TempFile(".", oname)
	if err != nil {
		fmt.Printf("error in creating temporary file\n")
	}
	var kva []KeyValue
	for _, file := range files {
		if strings.HasSuffix(file.Name(), strconv.Itoa(idx)) {
			ofile, err := os.Open(file.Name())
			if err != nil {
				fmt.Printf("error in opening file\n")
			}
			dec := json.NewDecoder(ofile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			ofile.Close()
		}
	}
	sort.Sort(ByKey(kva))
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
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	tempFile.Close()
	os.Rename(tempFile.Name(), oname)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// fmt.Printf("start working\n")
		var task Task
		var reply Reply
		status := call("Master.GetNextTask", &task, &reply)
		if !status {
			// fmt.Printf("worker finished\n")
			os.Exit(-1)
		}
		filename := reply.FileName
		nRed := reply.NReducers
		idx := reply.FileIdx

		// fmt.Printf("next task: %v\n", filename)
		// fmt.Println(reply.TT)
		task.TT = reply.TT
		if task.TT == mapTask {
			task.FileName = reply.FileName
		} else {
			task.FileName = strconv.Itoa(reply.FileIdx)
		}
		if reply.TT == idle {
			time.Sleep(time.Second * 2)
		} else {
			if reply.TT == mapTask {
				doMapTask(nRed, filename, mapf, idx)
			} else {
				doReduceTask(reducef, idx)
			}
			call("Master.DoneTask", &task, &reply)
		}

	}

}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	// fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	// fmt.Println(err)
	return false
}
