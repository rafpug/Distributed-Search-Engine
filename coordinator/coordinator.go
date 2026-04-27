package main

import (
	"distributed_search_engine/types"
	"fmt"
	"net"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

const reduceCount = 4

const B = 100
const maxUrls = 100000

/* Amount of seconds until a worker is considered dead */
const heartbeatExpire = 20 * time.Second

const replicaNum = 3

type CoordinatorAPI struct {
    mu sync.Mutex

    searchedURLS    map[string]bool
    urlQueue []string

    /* Keeps a map of workers to their jobs for tracking completion */
    mapWorkers map[string][]types.MapTask
    pendingMapTasks int
    pendingReduceTasks int

    reduceQueue []int

    /* Maps workers to the timestamp of their last heartbeat */
    heartbeatStamp map[string]time.Time

    /* Maps workers to their currently owned replicas */
    replicaTracker map[string][]string
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

    c.mapWorkers[workerId] = append(c.mapWorkers[workerId], newMap)

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

    c.reduceQueue = c.reduceQueue[1:]
    return &newReduce, nil
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
            if c.pendingMapTasks == 0 {
                /* No more map tasks */
                break
            }
            c.mu.Unlock()
            time.Sleep(10 * time.Millisecond)
            c.mu.Lock()
            continue
        }
        /* There is a map task ready to be assigned */
        c.pendingMapTasks++

        newMap := CreateMapTask(c, req.WorkerId)

        resp.TaskM = &newMap
        fmt.Printf("ASSIGN MAP TASK TO %s\n", req.WorkerId)
        return nil
    }

    /* Attempt to assign a reduce task */
    if len(c.reduceQueue) == 0 {
        if c.pendingReduceTasks == 0 {
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
            return nil
        }

        c.pendingReduceTasks++

        resp.TaskR = newReduce
        fmt.Printf("ASSIGN REDUCE TASK TO %s\n", req.WorkerId)
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
            c.searchedURLS[k] = true
        }
    }

    c.pendingMapTasks--
    fmt.Printf("GET MAP TASK RESULT\n")
    resp.Ok = true
    return nil
}

func (c *CoordinatorAPI) SortWorkersByLeastReplications() []string {
    workers := []string{}
    for k := range c.heartbeatStamp {
        /* Loop through every alive worker */
        workers = append(workers, k)
    }

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

        fmt.Printf("REPLICATE %s ON %s TO %s\n", outputFile, workerId, worker)
        c.replicaTracker[worker] = append(c.replicaTracker[worker], outputFile)
        neededReplicas--
    }
}

func (c *CoordinatorAPI) ReportReduceDone(req types.ReduceDoneRequest, resp *types.ReduceDoneResponse) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    c.InitiateReplicas(req.WorkerId, req.OutputFile)
    
    c.pendingReduceTasks--
    fmt.Printf("%s: finished their reduce step\n", req.WorkerId)
    return nil
}

func (c *CoordinatorAPI) RedoMapTasks(workerId string) {
    mapTasks := c.mapWorkers[workerId]
    delete(c.mapWorkers, workerId)
    delete(c.heartbeatStamp, workerId)

    for _, task := range mapTasks{
        for _, url := range task.Urls{
            delete(c.searchedURLS, url)
            c.urlQueue = append([]string{url}, c.urlQueue...)
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
                fmt.Println("Finished redoing maptasks")
                c.ReReplicateOutputs(workerId)
                fmt.Println("Finished rereplicating")
            }
        }
    })

    resp.Ok = true
    fmt.Printf("Recieved Heartbeat from: %s\n", req.WorkerId)
    return nil
}

func main() {
    coordAPI := &CoordinatorAPI{
        pendingMapTasks: 0,
        pendingReduceTasks: 0,
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
        mapWorkers: make(map[string][]types.MapTask),
        reduceQueue: make([]int, 0),
        heartbeatStamp: make(map[string]time.Time),
        replicaTracker: make(map[string][]string),
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
    // fmt.Printf("Reduce allocations: %v\n", allocs)

    for {
        conn, err := listener.Accept()
        if err != nil {
            continue
        }
        go rpc.ServeConn(conn)
    }
}
