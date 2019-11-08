package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// store immediate file handles
	var rFiles []*os.File
	for m := 0; m < nMap; m++ {
		rFileName := reduceName(jobName, m, reduceTask)
		// open reduce immediate file
		rFile, err := os.Open(rFileName)
		if err != nil {
			log.Fatalf("Create file err: %v", err)
		}
		rFiles = append(rFiles, rFile)
	}

	// 1. read from immediate files and decode them, change (k, v) -> (k, list(v))
	kvS := make(map[string][]string)
	for m := 0; m < nMap; m++ {
		dec := json.NewDecoder(rFiles[m])
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kvS[kv.Key] = append(kvS[kv.Key], kv.Value)
		}
	}

	// 2. sort keys
	var keys []string
	for k := range kvS {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	//log.Printf("length of kvS: %v", len(kvS))

	// 3. write result into outFile
	outF, err := os.Create(outFile)
	defer outF.Close()
	if err != nil {
		log.Fatalf("outFile open failed, err: %v", err)
	}
	enc := json.NewEncoder(outF)
	for _, key := range keys {
		err = enc.Encode(KeyValue{key, reduceF(key, kvS[key])})
		if err != nil {
			log.Fatalf("Encoder err: %v", err)
		}
	}

	// close the file
	for i := 0; i < nMap; i++ {
		err = rFiles[i].Close()
		if err != nil {
			log.Fatalf("Close file err: %v", err)
		}
	}

	return
}
