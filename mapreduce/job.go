package mapreduce

import "path"

type Job struct {
	N_Map int // the number of Map tasks
	N_Reduce int // the number of Reduce tasks
	InputPath string // the path of input data, like `examples/text.txt`
}

func (job *Job) InputDirName() string {
	return path.Dir(job.InputPath)
}

func (job *Job) InputFileName() string {
	return path.Base(job.InputPath)
}