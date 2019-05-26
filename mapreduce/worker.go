package mapreduce

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
)

// Worker process
type Worker struct {
	name string
	Reduce func(string, *list.List) string
	Map func(string) *list.List
	nRPC int
	nJobs int
	listener net.Listener
	master string
}

func (w *Worker) ExecuteJob(arg *ExecuteJobArgs, reply *ExecuteJobReply) error {
	fmt.Printf(
		"Operation: worker `%s` executes job #%d(input file: %s, operation: %v)\n",
		w.name, arg.JobNumber, arg.InputFile, arg.Operation,
	)
	switch arg.Operation {
	case OP_MAP:
		ExecuteMap(arg.JobNumber, arg.InputFile, arg.NumOtherPhase, w.Map)
	case OP_REDUCE:
		ExecuteReduce(arg.JobNumber, arg.InputFile, arg.NumOtherPhase, w.Reduce)
	}
	fmt.Printf("Operation: Executing %s on %s at %s",arg.Operation, arg.InputFile, w.name)
	reply.IsOk = true
	register(w.master, w.name)
	return nil
}

func (w *Worker) Shutdown(args *ShutdownArgs, reply *ShutdownReply) error {
	log.Println("Operation: Shutting down worker...")
	defer os.Exit(0)
	reply.NJobs = w.nJobs
	reply.IsOk = true
	w.nRPC = 1
	w.nJobs -= 1
	return nil
}

func register(master string, worker string) {
	args := &RegisterArgs{}
	args.WorkerName = worker
	var reply RegisterReply
	fmt.Printf("%s is registered on %s\n", worker, master)
	ok := call(master, "Master.Register", args, &reply)
	if !ok {
		fmt.Printf("Error: can not register worker %s on master %s\n", worker, master)
	}
}

func RunWorker(
	master string,
	worker string,
	Map func(string)*list.List,
	Reduce func(string, *list.List)string,
	nRPC int) {
		DPrintf("Operation: running a worker proceess at %s\n", worker)
		w := new(Worker)
		w.name = worker
		w.Map = Map
		w.Reduce = Reduce
		w.nRPC = nRPC
		w.master = master
		server := rpc.NewServer()
		server.Register(w)
		listener, err := net.Listen("tcp", worker)
		if err != nil {
			log.Fatalf("Error: can not start worker at %s: %s\n", worker, err)
		}
		w.listener = listener
		register(master, worker)
		for w.nRPC != 0 {
			conn, err := w.listener.Accept()
			if err == nil {
				w.nRPC -= 1
				go server.ServeConn(conn)
				w.nJobs += 1
			} else {
				break
			}
		}
		w.listener.Close()
		DPrintf("Operation: worker %s exit\n", worker)
}