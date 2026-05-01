package main

import (
	"distributed_search_engine/types"
	"fmt"
	"hash/fnv"
	"net"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

const reduceCount = 4

const B = 100
const maxUrls = 10000

/* Amount of seconds until a worker is considered dead */
const heartbeatExpire = 20 * time.Second

const replicaNum = 3

type CoordinatorAPI struct {
    mu sync.Mutex

    searchedURLS    map[string]bool
    urlQueue []string

    RemapQueue []types.MapTask

    /* Keeps a map of workers to their jobs for tracking completion */
    mapWorkers map[string][]types.MapTask
    mapIncompletion map[int]bool

    reduceQueue []int
    reduceIncompletion map[int]bool

    /* Maps workers to the timestamp of their last heartbeat */
    heartbeatStamp map[string]time.Time

    /* Maps workers to their currently owned replicas */
    replicaTracker map[string][]string

    taskNumber int
}

func CreateMapTask( c *CoordinatorAPI, workerId string) types.MapTask {
    count := B
    if len(c.urlQueue) < B {
        count = len(c.urlQueue)
    }
    urls := c.urlQueue[:count]
    c.urlQueue = c.urlQueue[count:]

    for _, url := range urls {
        c.searchedURLS[url] = true
    }

    intermFiles := []string{}

    for i := 0; i < reduceCount; i++ {
        s := fmt.Sprintf("%s-%d", workerId, i)
        intermFiles = append(intermFiles, s)
    }

    newMap := types.MapTask{
        R: reduceCount,
        Urls: urls,
        IntermFiles: intermFiles,
    }

    newMap.Id = c.taskNumber

    c.mapWorkers[workerId] = append(c.mapWorkers[workerId], newMap)
    c.mapIncompletion[c.taskNumber] = true
    c.taskNumber++

    return newMap
}

func CreateReduceTask(c *CoordinatorAPI, workerId string) (*types.ReduceTask, error) {
    reduceId := c.reduceQueue[0]

    intermFiles := []string {}
    for k, v := range c.mapWorkers {
        if k == workerId {
            fileName := v[0].IntermFiles[reduceId]
            intermFiles = append(intermFiles, fileName)
            continue
        }

        // fileName := fmt.Sprintf("%s-%d", k, reduceId)
        fileName := v[0].IntermFiles[reduceId]
        intermFiles = append(intermFiles, fileName)

        err := initTransfer(k, workerId, fileName)
        if err != nil {
            return nil, err
        }
        
    }
    
    newReduce := types.ReduceTask{
        Files: intermFiles,
        ReduceId: reduceId,
    }


    c.reduceIncompletion[reduceId] = true
    c.reduceQueue = c.reduceQueue[1:]
    return &newReduce, nil
}

func checkCompletion(dict map[int]bool) bool {
    for _, value := range dict {
        if value {
            return false
        }
    }
    return true
}

func (c *CoordinatorAPI) GetJob(req types.TaskRequest, resp *types.TaskResponse) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    resp.TaskM = nil
    resp.TaskR = nil
    resp.Done = false

    /* First attempt to assign a map task */
    for{
        if len(c.urlQueue) == 0 || len(c.searchedURLS) >= maxUrls {
            /* Attempt to reassign a map task from a failed worker when there are no normal map tasks left*/
            if len(c.RemapQueue) > 0 {
                oldTask := c.RemapQueue[0]
                
                intermFiles := []string{}
                for i := 0; i < reduceCount; i++ {
                    s := fmt.Sprintf("%s-%d", req.WorkerId, i)
                    intermFiles = append(intermFiles, s)
                }

                oldTask.IntermFiles = intermFiles
                c.mapIncompletion[oldTask.Id] = true
                c.mapWorkers[req.WorkerId] = append(c.mapWorkers[req.WorkerId], oldTask)
                resp.TaskM = &oldTask

                c.RemapQueue = c.RemapQueue[1:]
                fmt.Printf("REASSIGN MAP TASK %d TO %s\n", oldTask.Id, req.WorkerId)
                return nil
            }

            if checkCompletion(c.mapIncompletion) {
                /* No more map tasks */
                break
            }
            c.mu.Unlock()
            time.Sleep(10 * time.Millisecond)
            c.mu.Lock()
            continue
        }
        /* There is a map task ready to be assigned */

        newMap := CreateMapTask(c, req.WorkerId)
        resp.TaskM = &newMap



        fmt.Printf("ASSIGN MAP TASK %d TO %s\n", newMap.Id, req.WorkerId)
        return nil
    }

    /* Attempt to reassign a map task from a failed worker */


    /* Attempt to assign a reduce task */
    if len(c.reduceQueue) == 0 {
        if checkCompletion(c.reduceIncompletion) {
            // fmt.Println("SUCCESS")
            // resp.Done = true
        }
        c.mu.Unlock()
        time.Sleep(10 * time.Millisecond)
        c.mu.Lock()
    
    } else {
        /* Reduce task ready to be assigned */
        newReduce, err := CreateReduceTask(c, req.WorkerId)
        if err != nil {
            /* The file transfer to the reducing worker failed
                So we give up to let a different worker reattempt */
                c.mu.Unlock()
                time.Sleep(100 * time.Millisecond)
                c.mu.Lock()
            return nil
        }
        resp.TaskR = newReduce

        fmt.Printf("ASSIGN REDUCE TASK %d TO %s\n",newReduce.ReduceId, req.WorkerId)
    }
    return nil
}

func (c *CoordinatorAPI) ReportMapDone(req types.MapDoneRequest, resp *types.MapDoneResponse) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    for k := range req.Urls {
        if c.searchedURLS[k] {
            continue
        } else {
            c.urlQueue = append(c.urlQueue, k)
        }
    }

    delete(c.mapIncompletion, req.MapId)
    fmt.Printf("GET MAP TASK %d RESULT\n", req.MapId)
    resp.Ok = true
    return nil
}

func (c *CoordinatorAPI) SortWorkersByLeastReplications() []string {
    workers := []string{}

    c.mu.Lock()
    for k := range c.heartbeatStamp {
        /* Loop through every alive worker */
        workers = append(workers, k)
    }
    c.mu.Unlock()

    sort.Slice(workers, func(i, j int) bool {
        return len(c.replicaTracker[workers[i]]) < len(c.replicaTracker[workers[j]])
    })

    return workers
}

func initTransfer(sender string, reciever string, file string) error {
    address := fmt.Sprintf("rpc-%s:2001", sender)
    client, err := rpc.Dial("tcp", address)
    if err != nil {
        return err
    }

    recieverAddress := fmt.Sprintf("rpc-%s:2001", reciever)
    fileReq := types.InitTransferRequest{
        Address: recieverAddress,
        Filename: file,
    }

    fileResp := types.InitTransferResponse{}

    err = client.Call("WorkerAPI.InitiateFileTransfer", fileReq, &fileResp)
    client.Close()
    return err
}

func (c *CoordinatorAPI) InitiateReplicas(workerId string, outputFile string) {

    c.replicaTracker[workerId] = append(c.replicaTracker[workerId], outputFile)

    workers := c.SortWorkersByLeastReplications()

    neededReplicas := replicaNum - 1

    for _, worker := range workers{
        if worker == workerId {
            continue
        } else if neededReplicas <= 0 {
            break
        }

        err := initTransfer(workerId, worker, outputFile)

        if err != nil {
            continue
        }
        c.mu.Lock()
        fmt.Printf("REPLICATE %s ON %s TO %s\n", outputFile, workerId, worker)
        c.replicaTracker[worker] = append(c.replicaTracker[worker], outputFile)
        neededReplicas--
        c.mu.Unlock()
    }
}

func (c *CoordinatorAPI) ReportReduceDone(req types.ReduceDoneRequest, resp *types.ReduceDoneResponse) error {

    c.InitiateReplicas(req.WorkerId, req.OutputFile)
    
    c.mu.Lock()
    delete(c.reduceIncompletion, req.ReduceId)
    c.mu.Unlock()
    return nil
}

func (c *CoordinatorAPI) RedoMapTasks(workerId string) {
    c.mu.Lock()
    defer c.mu.Unlock()
    mapTasks := c.mapWorkers[workerId]
    delete(c.mapWorkers, workerId)
    delete(c.heartbeatStamp, workerId)

    for _, task := range mapTasks{
        c.RemapQueue = append(c.RemapQueue, task)
        if c.mapIncompletion[task.Id]{
            delete(c.mapIncompletion, task.Id)
        }
    }
}

func (c *CoordinatorAPI) ReReplicateOutputs(workerId string) {
    outputs := c.replicaTracker[workerId]
    
    for _, output := range outputs {
        workers := c.SortWorkersByLeastReplications()
        for _, worker := range workers {
            /* First check if this worker already owns a replica */
            replicas := c.replicaTracker[worker]

            found := false
            for _, replica := range replicas{
                if replica == output{
                    found = true
                }
            }

            if found {
                /* We want to spread out replicas, so we'll attempt to 
                    create a replica in a different worker */
                continue
            }

            /* We found a good worker to own the replica,
                so we attempt to transfer the replica */
            err := initTransfer(workerId, worker, output)
            if err != nil {
                continue
            } else {
                c.replicaTracker[worker] = append(c.replicaTracker[worker], output)
                fmt.Printf("REPLICATE %s ON %s TO %s\n", output, workerId, worker)
                break
            }
        }
    }

    delete(c.replicaTracker, workerId)
}

func (c *CoordinatorAPI) RecieveHeartbeat(req types.HeartbeatRequest, resp *types.HeartbeatResponse) error {
    c.mu.Lock()
    curTime := time.Now()
    c.heartbeatStamp[req.WorkerId] = curTime
    c.mu.Unlock()

    workerId := req.WorkerId

    time.AfterFunc(heartbeatExpire, func() {
        ts, ok := c.heartbeatStamp[workerId]

        if ok {
            heartbeatAge := time.Since(ts)
            if heartbeatAge >= heartbeatExpire {
                /* worker is dead */
                fmt.Printf("HEARTBEAT FAILED FROM %s\n", workerId)
                c.RedoMapTasks(workerId)
                c.ReReplicateOutputs(workerId)
            }
        }
    })

    resp.Ok = true
    return nil
}

func mrHash(key string, r int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff) % r
}

func (c *CoordinatorAPI) RedirectClient(req types.RedirectRequest, resp *types.RedirectResponse) error {

    outputId := mrHash(req.Keyword, reduceCount)
    outputName := fmt.Sprintf("output-%d", outputId)

    for worker, outputs := range c.replicaTracker {
        for _, output := range outputs {
            if outputName == output {
                workerAddress := fmt.Sprintf("rpc-%s:2001", worker)
                resp.Address = workerAddress
                resp.OutputFile = outputName
                return nil
            }
        }
    }

    return nil
}



func main() {
    coordAPI := &CoordinatorAPI{
        searchedURLS: make(map[string]bool, 0),
        urlQueue: []string{
            "https://en.wiktionary.org/wiki/Wiktionary:Main_Page",
            "https://en.wikipedia.org/wiki/Main_Page",
            "https://www.calpoly.edu/",
            "https://www.bbc.com/",
            "https://www.usa.gov/",
            "https://dmoztools.net/",
            "https://www.npr.org/",
        },
        RemapQueue: []types.MapTask{},
        mapWorkers: make(map[string][]types.MapTask),
        reduceQueue: make([]int, 0),
        mapIncompletion: make(map[int]bool),
        reduceIncompletion: make(map[int]bool),
        heartbeatStamp: make(map[string]time.Time),
        replicaTracker: make(map[string][]string),
        taskNumber: 0,
    }

    for i := 0; i < reduceCount; i++ {
        coordAPI.reduceQueue = append(coordAPI.reduceQueue, i)
    }

    rpc.Register(coordAPI)

    listener, err := net.Listen("tcp", ":3001")
    if err != nil {
        panic(err)
    }
    defer listener.Close()

    fmt.Printf("Coordinator listening\n")

    for {
        conn, err := listener.Accept()
        if err != nil {
            continue
        }
        go rpc.ServeConn(conn)
    }
}
