package mapreduce

import (
	"fmt"
	"net/rpc"
)

const (
	OP_MAP = "Map"
	OP_REDUCE = "Reduce"
)

// Debug
const DebugFlag = true
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugFlag {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type JobType string

// RPC arguments for execution of jobs
type ExecuteJobArgs struct {
	InputFile string
	Operation JobType
	JobNumber int
	NumOtherPhase int
}

// RPC reply for execution of jobs
type ExecuteJobReply struct {
	IsOk bool
}

// RPC call for shutdown
type Shutdown struct {}

// RPC arguments for shutdown call
type ShutdownArgs struct {}

// RPC reply for shutdown call
type ShutdownReply struct {
	NJobs int
	IsOk bool
}

// RPC arguments for worker registration
type RegisterArgs struct {
	WorkerName string
}

// RPC reply for worker registration
type RegisterReply struct {
	IsOk bool
}

// RPC arguments for submission of jobs
type SubmitJobArgs struct {
	Job Job
}

// RPC reply for submission of jobs
type SubmitJobReply struct {
	IsOk bool
}

// `call()` invokes remote procedure call to a server with arguments `args` and
// waits for the reply.
// `call()` returns true if the server responds, or false if `call()` was not able
// to contact with the server.
func call(server string, method string, args interface{}, reply interface{}) bool {

	// Dial up
	client, dialErr := rpc.Dial("tcp", server)
	if dialErr != nil {
		return false
	}
	defer client.Close()

	// invoke RPC
	callErr := client.Call(method, args, reply)
	if callErr != nil {
		fmt.Println(callErr)
		return false
	}
	return true
}