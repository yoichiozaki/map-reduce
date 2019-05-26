package mapreduce

import "fmt"

// Invoke job submission RPC for master
func SubmitJob(job Job, master string) {
	args := &SubmitJobArgs{Job: job}
	var reply SubmitJobReply
	if ok := call(master, "Master.SubmitJob", args, &reply); ok == false {
		fmt.Printf("SubmitJob: error when submit job to %s", master)
	}
}
