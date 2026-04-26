package main

import (
	"distributed_search_engine/types"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const reduceCount = 4

const B = 100
const maxUrls = 100000


type CoordinatorAPI struct {
    mu sync.Mutex

    searchedURLS    map[string]bool
    urlQueue []string
    // mapQueue []types.TaskResponse

    /* Keeps a map of workers to their jobs for tracking completion */
    mapWorkers map[string][]types.MapTask
    pendingMapTasks int
    pendingReduceTasks int

    reduceQueue []int

    // mapTasks        []types.MapTask
    // reduceTasks     []types.ReduceTask
    // mapAssigned     []bool
    // reduceAssigned  []bool
    // reduceDone      []bool
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

func CreateReduceTask(c *CoordinatorAPI, workerId string) types.ReduceTask {
    reduceId := c.reduceQueue[0]
    c.reduceQueue = c.reduceQueue[:1]

    intermFiles := []string {}
    for k, v := range c.mapWorkers {
        if k == workerId {
            fileName := v[0].IntermFiles[reduceId]
            intermFiles = append(intermFiles, fileName)
            continue
        }
        reducerAddress := fmt.Sprintf("rpc-%s:2001", workerId)

        // fileName := fmt.Sprintf("%s-%d", k, reduceId)
        fileName := v[0].IntermFiles[reduceId]
        intermFiles = append(intermFiles, fileName)

        address := fmt.Sprintf("rpc-%s:2001", k)

        client, err := rpc.Dial("tcp", address)
        if err != nil {
            panic(err)
        }
        fileReq := types.InitTransferRequest{
            Address: reducerAddress,
            Filename: fileName,
        }

        fileResp := types.InitTransferResponse{}

        err = client.Call("WorkerAPI.InitiateFileTransfer", fileReq, &fileResp)
        if err != nil {
            panic(err)
        }

        client.Close()
    }
    
    newReduce := types.ReduceTask{
        Files: intermFiles,
    }
    return newReduce
}

func (c *CoordinatorAPI) GetJob(req types.TaskRequest, resp *types.TaskResponse) error {
    fmt.Println("New get job request")
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
        fmt.Println("Successfully assigned map task from coord")
        return nil
    }

    /* Attempt to assign a reduce task */
    for{
        if len(c.reduceQueue) == 0 {
            if c.pendingReduceTasks == 0 {
                fmt.Println("SUCESSDOIHNE2")
                resp.Done = true
            }
            c.mu.Unlock()
            time.Sleep(10 * time.Millisecond)
            c.mu.Lock()
            break
        }
        /* Reduce task ready to be assigned */
        c.pendingReduceTasks++
        newReduce := CreateReduceTask(c, req.WorkerId)
        resp.TaskR = &newReduce
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

    resp.Ok = true
    return nil
}

func (c *CoordinatorAPI) ReportReduceDone(req types.ReduceDoneRequest, resp *types.ReduceDoneResponse) error {
    c.pendingReduceTasks--
    fmt.Printf("%s: finished their reduce step", req.WorkerId)
    /* Initiate replication of output files here */
    return nil
}

func main() {
    fmt.Printf("HelloWorld\n")

    // reduceTasks := buildReduceTasks(reduceCount)
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
    }

    for i := 0; i < reduceCount; i++ {
        coordAPI.reduceQueue[i] = i
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
