// package main

// import (
//     "fmt"
//     "net"
//     "net/rpc"
//     "strings"
//     "sync"
//     "file_map_reduce/types"
// )

// const reduceCount = 4
// const mapCount = 15
// const workerCount = 4

// type AllocRequest struct{}

// type AllocResponse struct {
//     Allocs []string
// }

// type ReduceAllocAPI struct {
//     Allocs []string
// }

// func (r *ReduceAllocAPI) SendAllocs(_ AllocRequest, resp *AllocResponse) error {
//     resp.Allocs = append([]string(nil), r.Allocs...)
//     return nil
// }

// type CoordinatorAPI struct {
//     mu sync.Mutex

//     mapTasks        []types.MapTask
//     reduceTasks     []types.ReduceTask
//     mapAssigned     []bool
//     mapDone         []bool
//     reduceAssigned  []bool
//     reduceDone      []bool
//     mapDoneCount    int
//     reduceDoneCount int
//     allocs          []string
// }

// func (c *CoordinatorAPI) GetJob(req types.TaskRequest, resp *types.TaskResponse) error {
//     c.mu.Lock()
//     defer c.mu.Unlock()

//     resp.TaskM = nil
//     resp.TaskR = nil
//     resp.Done = false

//     // Assign only map tasks owned by this worker.
//     for i, task := range c.mapTasks {
//         if c.mapDone[i] || c.mapAssigned[i] {
//             continue
//         }
//         if task.WorkerId != req.WorkerId {
//             continue
//         }

//         c.mapAssigned[i] = true
//         resp.TaskM = &task
//         fmt.Printf("Assigned map job %d to %s. file: %s\n", task.JobNum, req.WorkerId, task.InputFile)
//         return nil
//     }

//     // If this worker has unfinished or in-progress maps, wait.
//     for i, task := range c.mapTasks {
//         if task.WorkerId == req.WorkerId && !c.mapDone[i] {
//             fmt.Printf("Waiting on maps for %s\n", req.WorkerId)
//             return nil
//         }
//     }

//     // All map tasks are done globally before reduces begin.
//     if c.mapDoneCount < len(c.mapTasks) {
//         fmt.Printf("Waiting on maps for %s\n", req.WorkerId)
//         return nil
//     }

//     // Assign reduce tasks only to the worker that owns them.
//     for i, task := range c.reduceTasks {
//         if c.reduceDone[i] || c.reduceAssigned[i] {
//             continue
//         }

//         workerId := strings.Split(c.allocs[task.JobNum], ":")[0]
//         if workerId != req.WorkerId {
//             continue
//         }

//         c.reduceAssigned[i] = true
//         resp.TaskR = &task
//         fmt.Printf("Assigned reduce job %d to %s\n", task.JobNum, req.WorkerId)
//         return nil
//     }

//     if c.reduceDoneCount == len(c.reduceTasks) {
//         resp.Done = true
//         fmt.Printf("No jobs for %s\n", req.WorkerId)
//     }

//     return nil
// }

// func (c *CoordinatorAPI) ReportMapDone(req types.JobDoneRequest, resp *string) error {
//     c.mu.Lock()
//     c.mapDoneCount++
//     c.mu.Unlock()

//     *resp = "ok"
//     return nil
// }

// func (c *CoordinatorAPI) ReportReduceDone(req types.JobDoneRequest, resp *string) error {
//     c.mu.Lock()
//     c.reduceDoneCount++
//     c.mu.Unlock()

//     *resp = "ok"
//     return nil
// }

// // pre-determine reducer allocations
// func buildAllocs(reduceCount int, workerCount int) []string {
//     allocs := make([]string, reduceCount)
//     for i := range reduceCount {
//         allocs[i] = fmt.Sprintf("worker%d:2001", (i%workerCount)+1)
//     }
//     return allocs
// }

// func buildMapTasks(count int, reduceCount int) []types.MapTask {
//     tasks := make([]types.MapTask, 0, count)

//     for i := 0; i < count; i++ {
//         workerID := fileOwner(i)

//         tasks = append(tasks, types.MapTask{
//             JobNum:    i,
//             InputFile: fmt.Sprintf("/input/%d", i),
//             R:         reduceCount,
//             WorkerId:  workerID,
//         })
//     }

//     return tasks
// }

// func fileOwner(i int) string {
//     switch {
//     case i >= 0 && i <= 3:
//         return "worker1"
//     case i >= 4 && i <= 7:
//         return "worker2"
//     case i >= 8 && i <= 11:
//         return "worker3"
//     default:
//         return "worker4"
//     }
// }

// func buildReduceTasks(count int) []types.ReduceTask {
//     tasks := make([]types.ReduceTask, 0, count)
//     for i := range count {
//         tasks = append(tasks, types.ReduceTask{JobNum: i})
//     }
//     return tasks
// }

// func main() {
//     allocs := buildAllocs(reduceCount, workerCount)

//     allocAPI := &ReduceAllocAPI{Allocs: allocs}
//     coordAPI := &CoordinatorAPI{
//         mapTasks: buildMapTasks(mapCount, reduceCount),
//         reduceTasks: buildReduceTasks(reduceCount),
//         allocs: allocs,
//     }

//     rpc.Register(allocAPI)
//     rpc.Register(coordAPI)

//     listener, _ := net.Listen("tcp", ":3001")
//     defer listener.Close()

//     fmt.Printf("Coordinator listening\n")
//     fmt.Printf("Reduce allocations: %v\n", allocs)

//     for {
//         conn, _ := listener.Accept()
//         go rpc.ServeConn(conn)
//     }
// }

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

    mapTasks        []types.MapTask
    reduceTasks     []types.ReduceTask
    mapAssigned     []bool
    mapDone         []bool
    reduceAssigned  []bool
    reduceDone      []bool
    mapDoneCount    int
    reduceDoneCount int
    allocs          []string
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

func (c *CoordinatorAPI) ReportMapDone(req types.JobDoneRequest, resp *string) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    if req.JobNum >= 0 && req.JobNum < len(c.mapDone) && !c.mapDone[req.JobNum] {
        c.mapDone[req.JobNum] = true
        c.mapDoneCount++
    }

    *resp = "ok"
    return nil
}

func (c *CoordinatorAPI) ReportReduceDone(req types.JobDoneRequest, resp *string) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    if req.JobNum >= 0 && req.JobNum < len(c.reduceDone) && !c.reduceDone[req.JobNum] {
        c.reduceDone[req.JobNum] = true
        c.reduceDoneCount++
    }

    *resp = "ok"
    return nil
}

// pre-determine reducer allocations
func buildAllocs(reduceCount int, workerCount int) []string {
    allocs := make([]string, reduceCount)
    for i := 0; i < reduceCount; i++ {
        allocs[i] = fmt.Sprintf("worker%d:2001", (i%workerCount)+1)
    }
    return allocs
}

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
