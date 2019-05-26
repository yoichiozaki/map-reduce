package mapreduce

import "fmt"

func DoShutdown(master string) {
	args := &ShutdownArgs{}
	var reply ShutdownReply
	if ok := call(master, "Master.Shutdown", args, &reply); ok == false {
		fmt.Printf("DoShutdown: error when shutdown master %s", master)
	}
}
