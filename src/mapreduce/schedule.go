package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	/**
		func (wk *Worker) register(master string) {
		args := new(RegisterArgs)
		args.Worker = wk.name
		ok := call(master, "Master.Register", args, new(struct{}))
		if ok == false {
			fmt.Printf("Register: RPC %s register error\n", master)
		}
	}
	*/

	var wg sync.WaitGroup
	wg.Add(ntasks)

	taskDoneCh := make(chan struct{})
	taskNumCh := make(chan int, ntasks) // store failure task num

	// assign task given to the failed worker to another worker
	for i := 0; i < ntasks; i++ {
		taskNumCh <- i
	}

	// 1. listen registerChan and taskNumCh to allocate task to worker
	go func() {
	Loop:
		for {
			select {
			case <-taskDoneCh:
				break Loop
			default:
				num := <-taskNumCh
				worker := <-registerChan
				go func(num int) {
					args := new(DoTaskArgs)
					args.File = mapFiles[num]
					args.JobName = jobName
					args.Phase = phase
					args.NumOtherPhase = n_other
					args.TaskNumber = num
					ok := call(worker, "Worker.DoTask", args, nil)
					if ok == false {
						fmt.Printf("DoTask: RPC %s call error", worker)
						// 2. store failure task num into taskNumCh
						taskNumCh <- num
						wg.Add(1)
					}
					wg.Done()
					// 3. add worker that has done task into registerChan
					registerChan <- worker
				}(num)

			}
		}
	}()

	/*	for i := 0; i < ntasks; i++ {
		// 1. listen registerChan and allocate task to worker
		worker := <- registerChan
		go func(num int) {
			args := new(DoTaskArgs)
			args.File = mapFiles[num]
			args.JobName = jobName
			args.Phase = phase
			args.NumOtherPhase = n_other
			args.TaskNumber = num
			ok := call(worker, "Worker.DoTask", args, nil)
			if ok == false {
				fmt.Printf("DoTask: RPC %s call error", worker)
				// store failure task num
				taskNumCh <- num
				wg.Add(1)
			}
			wg.Done()
			// 2. add worker that has done task into registerChan
			registerChan <- worker
		}(i)
	}*/

	wg.Wait()

	taskDoneCh <- struct{}{}

	fmt.Printf("Schedule: %v done\n", phase)
}
