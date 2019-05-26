package mapreduce

import (
	"bufio"
	"container/list"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
)

type KeyValue struct {
	Key string
	Value string
}

type MapReduce struct {
	job Job // the job to be executed
	nMap int
	nReduce int
	inputFile string
}

func InitMapReduce(job Job) *MapReduce {
	mr := new(MapReduce)
	mr.job = job
	mr.nMap = job.N_Map
	mr.nReduce = job.N_Reduce
	mr.inputFile = job.InputPath
	return mr
}

// `MapName()` returns the name of input file for map job #<MapJob>
func MapName(filePath string, MapJob int) string {
	dir := path.Dir(filePath)
	file := path.Base(filePath)
	return path.Join(dir, "mapreduce-tmp." + file + "-" + strconv.Itoa(MapJob))
}

// `Split()` splits an input file into nMap pieces
func (mr *MapReduce) Split(filePath string) {
	fmt.Printf("Split: %s\n", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Error: can not open %s: %s\n", filePath, err)
	}

	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("Error: can not get stat of %s: %s\n", filePath, err)
	}
	size := fileInfo.Size()
	chunkNum := size / int64(mr.nMap)
	chunkNum += 1

	mOut, err := os.Create(MapName(filePath, 0))
	if err != nil {
		log.Fatalf("Error: can not create output file for map operation: %s\n", err)
	}

	writer := bufio.NewWriter(mOut)
	m := 1
	i := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if int64(i) > chunkNum*int64(m) {
			writer.Flush()
			mOut.Close()
			mOut, err = os.Create(MapName(filePath, m))
			writer = bufio.NewWriter(mOut)
			m += 1
		}
		line := scanner.Text() + "\n"
		writer.WriteString(line)
		i += len(line)
	}
	writer.Flush()
	mOut.Close()
}

// `ReduceName()` returns the name of processed file for reduce job #<ReduceJob>
func ReduceName(filePath string, MapJob int, ReduceJob int) string {
	mapFile := MapName(filePath, MapJob)
	dir := path.Dir(mapFile)
	file := path.Base(mapFile)
	return path.Join(dir, file + "-" + strconv.Itoa(ReduceJob))
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func ExecuteMap(JobNumber int, fileName string, nReduce int, Map func(string)*list.List) {
	mapName := MapName(fileName, JobNumber)
	file, err := os.Open(mapName)
	if err != nil {
		log.Fatalf("Error: can not `ExecuteMap()`: %s\n", err)
	}
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("Error: can not get stat of %s: %s\n", mapName, err)
	}
	size := fileInfo.Size()
	fmt.Printf("Operation: reading splitted %s(%d byte)\n", mapName, size)
	buf := make([]byte, size)
	_, err = file.Read(buf)
	if err != nil {
		log.Fatalf("Error: can not read %s: %s\n", mapName, err)
	}
	file.Close()
	mapped := Map(string(buf))
	for r := 0; r < nReduce; r++ {
		file, err := os.Create(ReduceName(fileName, JobNumber, r))
		if err != nil {
			log.Fatalf("Error: can not create intermediate file %s: %s\n", ReduceName(fileName, JobNumber, r), err)
		}
		jsonEncoder := json.NewEncoder(file)
		for ele := mapped.Front(); ele != nil; ele = ele.Next() {
			kv := ele.Value.(KeyValue)
			if hash(kv.Key)%uint32(nReduce) == uint32(r) {
				err := jsonEncoder.Encode(&kv)
				if err != nil {
					log.Fatalln("Error: can not marshall: ", err)
				}
			}
		}
		file.Close()
	}
}

func MergeName(fileName string, ReduceJob int) string {
	dir := path.Dir(fileName)
	file := path.Base(fileName)
	return path.Join(dir, "mapreduce-tmp." + file + "-result-" + strconv.Itoa(ReduceJob))
}

func ExecuteReduce(job int, fileName string, nMap int, Reduce func(string, *list.List)string) {
	kvs := make(map[string]*list.List)
	for i := 0; i < nMap; i++ {
		name := ReduceName(fileName, i, job)
		fmt.Printf("Operation: reading itermidiate file %s\n", name)
		file, err := os.Open(name)
		if err != nil {
			log.Fatalf("Error: can not read intermidiate file %s: %s\n", name, err)
		}
		jsonDecoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = jsonDecoder.Decode(&kv)
			if err != nil {
				break
			}
			_, ok := kvs[kv.Key]
			if !ok {
				kvs[kv.Key] = list.New()
			}
			kvs[kv.Key].PushBack(kv.Value)
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	reducedFile := MergeName(fileName, job)
	file, err := os.Create(reducedFile)
	if err != nil {
		log.Fatalf("Error: can not create output file %s for reduce operation: %s", reducedFile, err)
	}
	jsonEncoder := json.NewEncoder(file)
	for _, k := range keys {
		reduced := Reduce(k, kvs[k])
		jsonEncoder.Encode(KeyValue{k, reduced})
	}
	file.Close()
}

func OutputName(filePath string) string {
	dir := path.Dir(filePath)
	file := path.Base(filePath)
	return path.Join(dir, "mapreduce-tmp." + file)
}

func (mr *MapReduce) Merge() {
	DPrintf("Mergeing...")
	kvs := make(map[string]string)
	for i := 0; i < mr.nReduce; i++ {
		p := MergeName(mr.inputFile, i)
		fmt.Printf("Operation: read %s\n", p)
		file, err := os.Open(p)
		if err != nil {
			log.Fatalf("Error: can not open %s: %s", p, err)
		}
		jsonDecoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := jsonDecoder.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	file, err := os.Create(OutputName(mr.inputFile))
	if err != nil {
		log.Fatalf("Error: can not create output file %s: %s", OutputName(mr.inputFile), err)
	}
	writer := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(writer, "%s\t%s\n", k, kvs[k])
	}
	writer.Flush()
	file.Close()
}

func remove(fileName string) {
	err := os.Remove(fileName)
	if err != nil {
		log.Fatalf("Error: can not remove %s: %s", fileName, err)
	}
}

func (mr *MapReduce) CleanUpAllFiles() {
	for i := 0; i < mr.nMap; i++ {
		remove(MapName(mr.inputFile, i))
		for j := 0; j < mr.nReduce; j++ {
			remove(ReduceName(mr.inputFile, i, j))
		}
	}
	for i := 0; i < mr.nReduce; i++ {
		remove(MergeName(mr.inputFile, i))
	}
	remove("mapreduce-tmp." + mr.inputFile)
}

func RunSequentially(
	job Job,
	Map func(string)*list.List,
	Reduce func(string, *list.List) string) {
	mr := InitMapReduce(job)
	mr.Split(mr.inputFile)

	// Execute `Map()` sequentially
	for i := 0; i < mr.nMap; i++ {
		ExecuteMap(i, mr.inputFile, mr.nReduce, Map)
	}

	// Execute `Reduce()` sequentially
	for i := 0; i < mr.nReduce; i++ {
		ExecuteReduce(i, mr.inputFile, mr.nMap, Reduce)
	}

	// Merge the results of reduce operations to get a result
	mr.Merge()
}