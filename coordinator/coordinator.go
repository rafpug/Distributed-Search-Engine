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
const mapCount = 15

const B = 100
const maxUrls = 1000


type CoordinatorAPI struct {
    mu sync.Mutex

    searchedURLS    map[string]bool
    urlQueue []string
    // mapQueue []types.TaskResponse

    /* Keeps a map of workers to their jobs for tracking completion */
    mapWorkers map[string][]types.MapTask
    pendingMapTasks int

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

    intermFiles := make([]string, reduceCount)

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
    fmt.Println("SUCESSDOIHNE2")
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

    c.pendingMapTasks--

    resp.Ok = true
    return nil
}

// func (c *CoordinatorAPI) ReportReduceDone(req types.JobDoneRequest, resp *string) error {
//     c.mu.Lock()
//     defer c.mu.Unlock()

//     if req.JobNum >= 0 && req.JobNum < len(c.reduceDone) && !c.reduceDone[req.JobNum] {
//         c.reduceDone[req.JobNum] = true
//         c.reduceDoneCount++
//     }

//     *resp = "ok"
//     return nil
// }

// func buildReduceTasks(count int) []types.ReduceTask {
//     tasks := make([]types.ReduceTask, 0, count)
//     for i := 0; i < count; i++ {
//         tasks = append(tasks, types.ReduceTask{JobNum: i})
//     }
//     return tasks
// }

func main() {
    fmt.Printf("HelloWorld\n")

    // reduceTasks := buildReduceTasks(reduceCount)
    coordAPI := &CoordinatorAPI{
        pendingMapTasks: 0,
        searchedURLS: make(map[string]bool, 0),
        urlQueue: []string{
            "https://en.wiktionary.org/wiki/Wiktionary:Main_Page",
        },
        mapWorkers: make(map[string][]types.MapTask),
        

        // reduceTasks:    reduceTasks,
        // reduceAssigned: make([]bool, len(reduceTasks)),
        // reduceDone:     make([]bool, len(reduceTasks)),
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
