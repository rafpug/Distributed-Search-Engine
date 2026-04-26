package main

import (
	"distributed_search_engine/types"
	"fmt"
	"net"
	"net/rpc"
	"strings"
	"sync"
)

const reduceCount = 4
const mapCount = 15
const workerCount = 4

type AllocRequest struct{}

type AllocResponse struct {
    Allocs []string
}

type ReduceAllocAPI struct {
    Allocs []string
}

func (r *ReduceAllocAPI) SendAllocs(_ AllocRequest, resp *AllocResponse) error {
    resp.Allocs = append([]string(nil), r.Allocs...)
    return nil
}

type CoordinatorAPI struct {
    mu sync.Mutex

    searchedURLS    map[string]bool
    urlQueue []string
    mapQueue []types.TaskResponse

    mapTasks        []types.MapTask
    reduceTasks     []types.ReduceTask
    mapAssigned     []bool
    reduceAssigned  []bool
    reduceDone      []bool
}

func containsInt(nums []int, target int) bool {
	for _, n := range nums {
		if n == target {
			return true
		}
	}
	return false
}

func (c *CoordinatorAPI) GetJob(req types.TaskRequest, resp *types.TaskResponse) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    resp.TaskM = nil
    resp.TaskR = nil
    resp.Done = false

    // Assign only map tasks owned by this worker.
    for i, task := range c.mapTasks {
        if c.mapDone[i] || c.mapAssigned[i] {
            continue
        }
        if !containsInt(req.Files, task.JobNum) {
            continue
        }

        c.mapAssigned[i] = true
        resp.TaskM = &task
        fmt.Printf("Assigned map job %d to %s. file: %s\n", task.JobNum, req.WorkerId, task.InputFile)
        return nil
    }

    // If this worker has unfinished or in-progress maps, wait.
    // for i, task := range c.mapTasks {
    //     if task.WorkerId == req.WorkerId && !c.mapDone[i] {
    //         fmt.Printf("Waiting on maps for %s\n", req.WorkerId)
    //         return nil
    //     }
    // }

    // All map tasks are done globally before reduces begin.
    if c.mapDoneCount < len(c.mapTasks) {
        fmt.Printf("Waiting on maps for %s\n", req.WorkerId)
        return nil
    }

    // Assign reduce tasks only to the worker that owns them.
    for i, task := range c.reduceTasks {
        if c.reduceDone[i] || c.reduceAssigned[i] {
            continue
        }

        if task.JobNum >= len(c.allocs) {
            continue
        }

        workerId := strings.Split(c.allocs[task.JobNum], ":")[0]
        if workerId != req.WorkerId {
            continue
        }

        c.reduceAssigned[i] = true
        resp.TaskR = &task
        fmt.Printf("Assigned reduce job %d to %s\n", task.JobNum, req.WorkerId)
        return nil
    }

    if c.reduceDoneCount == len(c.reduceTasks) {
        resp.Done = true
        fmt.Printf("No jobs for %s\n", req.WorkerId)
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


func buildMapTasks(count int, reduceCount int) []types.MapTask {
    tasks := make([]types.MapTask, 0, count)

    for i := 0; i < count; i++ {
        tasks = append(tasks, types.MapTask{
            JobNum: i,
            InputFile: fmt.Sprintf("/data%d", i),
            R: reduceCount,
        })
    }

    return tasks
}

func buildReduceTasks(count int) []types.ReduceTask {
    tasks := make([]types.ReduceTask, 0, count)
    for i := 0; i < count; i++ {
        tasks = append(tasks, types.ReduceTask{JobNum: i})
    }
    return tasks
}

func main() {
    fmt.Printf("HelloWorld\n")
    allocs := buildAllocs(reduceCount, workerCount)

    mapTasks := buildMapTasks(mapCount, reduceCount)
    reduceTasks := buildReduceTasks(reduceCount)

    allocAPI := &ReduceAllocAPI{Allocs: allocs}
    coordAPI := &CoordinatorAPI{
        mapTasks:       mapTasks,
        reduceTasks:    reduceTasks,
        mapAssigned:    make([]bool, len(mapTasks)),
        mapDone:        make([]bool, len(mapTasks)),
        reduceAssigned: make([]bool, len(reduceTasks)),
        reduceDone:     make([]bool, len(reduceTasks)),
        allocs:         allocs,
    }

    rpc.Register(allocAPI)
    rpc.Register(coordAPI)

    listener, err := net.Listen("tcp", ":3001")
    if err != nil {
        panic(err)
    }
    defer listener.Close()

    fmt.Printf("Coordinator listening\n")
    fmt.Printf("Reduce allocations: %v\n", allocs)

    for {
        conn, err := listener.Accept()
        if err != nil {
            continue
        }
        go rpc.ServeConn(conn)
    }
}
