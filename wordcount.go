package main

import (
	"container/list"
	"fmt"
	"map-reduce/mapreduce"
	"os"
	"strconv"
	"strings"
	"unicode"
)

func Map(value string) *list.List {
	isNotLetter := func(r rune) bool {
		return !unicode.IsLetter(r)
	}
	fields := strings.FieldsFunc(value, isNotLetter)
	l := list.New()
	for _, f := range fields {
		kv := mapreduce.KeyValue{Key: f, Value: "1"}
		l.PushBack(kv)
	}
	return l
}

func Reduce(key string, values *list.List) string {
	count := 0
	for ele := values.Front(); ele != nil; ele = ele.Next() {
		v := ele.Value.(string)
		intValue, _ := strconv.Atoi(v)
		count += intValue
	}
	return strconv.Itoa(count)
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("%s: Follow the instructions below\n", os.Args[0])
		fmt.Println("1) Run Master (e.g., `go run wordcount.go master localhost:7777`)")
		fmt.Println("2) Run Workers (e.g., `go run wordcount.go worker localhost:7777 localhost:7778`)")
		fmt.Println("3) Give the number of mapper/worker and submit a word count job for the master working on given address " +
			"(e.g., `go run wordcount.go submit ./input/bible localhost:7777 3 5`)")
		fmt.Println("4) Terminate Master and Workers (e.g., `go run wordcount.go terminate localhost:7777`")
		fmt.Println("Also, you can try to run sequentially (e.g., `go run wordcount.go sequential ./input/bible`)." +
			"This time there are 5 workers for map, 3 worker for reduce")
		return
	}
	switch os.Args[1] {
	case "sequential":
		job := mapreduce.Job{
			N_Map: 5,
			N_Reduce:3,
			InputPath: os.Args[2],
		}
		mapreduce.RunSequentially(job, Map, Reduce)
	case "master":
		mapreduce.RunMaster(os.Args[2])
	case "worker":
		mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, -1)
	case "submit":
		job := mapreduce.Job{
			N_Map: atoi(os.Args[4]),
			N_Reduce: atoi(os.Args[5]),
			InputPath: os.Args[2],
		}
		mapreduce.SubmitJob(job, os.Args[3])
	case "terminate":
		mapreduce.DoShutdown(os.Args[2])
	}
}

func atoi(a string) int {
	i, _ := strconv.Atoi(a)
	return i
}