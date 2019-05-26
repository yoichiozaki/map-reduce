package mapreduce

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
)

// Information to manage workers
type WorkerInfo struct {
	address string
}

// Master process
type Master struct {
	Address string // like `localhost:7777`
	registerChannel chan string
	mappedChannel chan bool
	reducedChannel chan bool
	submittedChannel chan bool
	listener net.Listener
	isAlive bool
	status *list.List
	job Job // Job to be executed
	Workers map[string]*WorkerInfo // manages workers' states
}

// Exposed as RPC `Master.Register`
func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	DPrintf("Operation: registering worker %s\n", args.WorkerName)
	worker := args.WorkerName
	m.registerChannel <- worker
	m.Workers[worker] = &WorkerInfo{address: worker}
	reply.IsOk = true
	return nil
}

// Exposed as RPC `Master.Shutdown`
func (m *Master) Shutdown(args *ShutdownArgs, reply *ShutdownReply) error {
	DPrintf("Operation: shutdowning worker registration server\n")
	defer os.Exit(0)
	m.isAlive = false
	m.listener.Close()
	m.killAllWorkers()
	return nil
}

// Exposed as RPC `Master.SubmitJob`
func (m *Master) SubmitJob(args *SubmitJobArgs, reply *SubmitJobReply) error {
	DPrintf("Operation: submit")
	m.job = args.Job
	reply.IsOk = true
	m.submittedChannel <- true
	return nil
}

// Start `Master` as a RPC server
func (m *Master) Start() {
	master := rpc.NewServer()
	master.Register(m)
	listener, err := net.Listen("tcp", m.Address)
	if err != nil {
		log.Fatal("Error: starting master at ", m.Address, " :", err)
	}
	m.listener = listener

	// now listening on the master address...

	go func() {
		for m.isAlive {
			// Accept new connection from workers here
			conn, err := m.listener.Accept()
			if err == nil {
				go func() {
					master.ServeConn(conn)
					conn.Close()
				}()
			} else {
				DPrintf("Error: accept error", err)
				break
			}
		}
		DPrintf("Operation: Master process is working now!\n")
	}()
}

func (m *Master) CleanUpAllRegistrations() {
	args := &ShutdownArgs{}
	var reply ShutdownReply
	ok := call(m.Address, "Master.Shutdown", args, &reply)
	if !ok {
		fmt.Printf("Error: `CleanUpMasterRegistration()`")
		return
	}
	DPrintf("Operation: Clean up all registrations")
}

// Kill all the workers by sending a Shutdown RPCs and
// collect the number of jobs each worker has executed.
func (m *Master) killAllWorkers() *list.List {
	l := list.New()
	for _, worker := range m.Workers {
		DPrintf("Operation: shutdowning %s\n", worker.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		if ok := call(worker.address, "Worker.Shutdown", args, &reply); !ok {
			fmt.Printf("Error: RPC %s shutdown error\n", worker.address)
		} else {
			l.PushBack(reply.NJobs)
		}
	}
	return l
}

func (m *Master) CallMaps(worker string, jobNum int) {
	args := ExecuteJobArgs{
		InputFile: m.job.InputPath,
		Operation: OP_MAP,
		JobNumber: jobNum,
		NumOtherPhase: m.job.N_Reduce,
	}
	reply := new(ExecuteJobReply)
	call(worker, "Worker.ExecuteJob", args, &reply)
	m.mappedChannel <- true
}

func (m *Master) CallReduces(worker string, jobNum int) {
	args := ExecuteJobArgs{
		InputFile: m.job.InputPath,
		Operation: OP_REDUCE,
		JobNumber: jobNum,
		NumOtherPhase: m.job.N_Map,
	}
	reply := new(ExecuteJobReply)
	call(worker, "Worker.ExecuteJob", args, &reply)
	m.reducedChannel <- true
}

func (m *Master) ExecuteJob() *list.List {
	nMap := m.job.N_Map
	nReduce := m.job.N_Reduce
	for i := 0; i < nMap; i++ {
		worker := <- m.registerChannel
		go m.CallMaps(worker, i)
	}
	for i := 0; i < nMap; i++ {
		<- m.mappedChannel
	}
	fmt.Println("Operation: all maps are done!")

	for i := 0; i < nReduce; i++ {
		worker := <- m.registerChannel
		fmt.Println("worker: " +  worker)
		go m.CallReduces(worker, i)
	}
	for i := 0; i < nReduce; i++ {
		<- m.reducedChannel
	}

	fmt.Println("Operation: all reduces are done!")
	return nil
}

func (m *Master) Run() {
	fmt.Println("Waiting for any jobs...")
	<- m.submittedChannel
	input := m.job.InputPath
	fmt.Printf("Operation: run mapreduce job at %s for %s", m.Address, input)
	mr := InitMapReduce(m.job)
	mr.Split(input)
	m.ExecuteJob()
	mr.Merge()
	fmt.Printf("Operation: done mapreduce at %s\n", m.Address)
}

func InitMaster(address string) *Master {
	m := new(Master)
	m.Address = address
	m.isAlive = true
	m.registerChannel = make(chan string, 100)
	m.mappedChannel = make(chan bool, 100)
	m.reducedChannel = make(chan bool, 100)
	m.submittedChannel = make(chan bool)
	m.Workers = make(map[string]*WorkerInfo)
	return m
}

func (m *Master) ListenOnExit() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			log.Printf("Notify: captured %v, exiting...", sig)
			m.killAllWorkers()
			os.Exit(1)
		}
	}()
}

func RunMaster(address string) {
	m := InitMaster(address)
	m.Start()
	m.ListenOnExit()
	for {
		m.Run()
	}
}