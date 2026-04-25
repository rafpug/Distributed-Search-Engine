package main

import (
	"bufio"
	"distributed_search_engine/types"
	"fmt"
	"hash/fnv"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"unicode"
)

type KeyValue struct {
	Key   string
	Value int
}

type MapResultBatch struct {
	ReduceID int
	Data     []KeyValue
}

type AllocRequest struct{}

type AllocResponse struct {
	Allocs []string
}

type PeerAPI struct{}

var myName string
var reducerAllocs []string
var incomingReduceData = make(map[int][]KeyValue)
var incomingMu sync.Mutex
var sentBatches = make(map[int]int)
var sentBatchesMu sync.Mutex

/* Distributes the given key words across the intermediate files */
func mrHash(key string, r int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff) % r
}

// makes word lowercase and removes no alphanumeric chars from beginning/end
func normalizeWord(word string) string {
	word = strings.ToLower(word)
	return strings.TrimFunc(word, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
}

// save incoming mappings to be reduced
func (p *PeerAPI) ReceiveBatch(args MapResultBatch, response *string) error {
	incomingMu.Lock()
	incomingReduceData[args.ReduceID] = append(incomingReduceData[args.ReduceID], args.Data...)
	incomingMu.Unlock()

	*response = "ok"
	return nil
}

// listen for incoming mappings to be reduced
func listenForPeerRPC() {
	peer_api := new(PeerAPI)
	rpc.Register(peer_api)

	listener, _ := net.Listen("tcp", ":2001")

	for {
		conn, _ := listener.Accept()
		go rpc.ServeConn(conn)
	}
}

// send completed mappings to be reduced
func sendData(reduceID int, batch []KeyValue, jobNum int, coordinatorClient *rpc.Client) {
	addr := reducerAllocs[reduceID]

	client, _ := rpc.Dial("tcp", addr)
	defer client.Close()

	req := MapResultBatch{
		ReduceID: reduceID,
		Data:     batch,
	}

	resp := ""
	client.Call("PeerAPI.ReceiveBatch", req, &resp)

	sentBatchesMu.Lock()
	sentBatches[jobNum]--
	if sentBatches[jobNum] == 0{
		sentBatchesMu.Unlock()
		reportMapDone(jobNum, coordinatorClient)
	}else{
		sentBatchesMu.Unlock()
	}
}

// preforms map operation
func mapData(jobNum int, dataPath string, reducerCount int, coordinatorClient *rpc.Client) {
	input, _ := os.Open(dataPath)
	defer input.Close()

	mapped := make(map[string]int)
	scanner := bufio.NewScanner(input)

	// count words
	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Fields(line)
		for _, word := range words {
			word = normalizeWord(word)
			if word == "" {
				continue
			}
			mapped[word]++
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	// split data into reduce batches
	partitions := make([][]KeyValue, reducerCount)
	for key, value := range mapped {
		reduceID := mrHash(key, reducerCount)
		partitions[reduceID] = append(partitions[reduceID], KeyValue{
			Key:   key,
			Value: value,
		})
	}

	// send data
	for reduceID, batch := range partitions {
		if len(batch) == 0 {
			continue
		}
		sentBatchesMu.Lock()
		sentBatches[jobNum]++
		sentBatchesMu.Unlock()
		go sendData(reduceID, batch, jobNum, coordinatorClient)
	}

	fmt.Printf("%s finished map job on %s\n", myName, dataPath)
}

// preforms reduce operation
func reduceData(jobNum int) {
	// get data for specific job
	incomingMu.Lock()
	data := make([]KeyValue, len(incomingReduceData[jobNum]))
	copy(data, incomingReduceData[jobNum])
	incomingMu.Unlock()

	// reduce data
	reduced := make(map[string]int)
	for _, kv := range data {
		reduced[kv.Key] += kv.Value
	}

	// write to file
	outName := fmt.Sprintf("/output/reduce_out_%d.txt", jobNum)
	output, err := os.Create(outName)
	if err != nil {
		panic(err)
	}
	defer output.Close()

	for key, value := range reduced {
		fmt.Fprintf(output, "%s %d\n", key, value)
	}

	fmt.Printf("%s wrote output for job %d to %s\n", myName, jobNum, outName)
}

// Get assigned worker #s for reducer tasks
func getReducerAllocs(client *rpc.Client) {
	req := AllocRequest{}
	resp := AllocResponse{}

	client.Call("ReduceAllocAPI.SendAllocs", req, &resp)

	reducerAllocs = make([]string, len(resp.Allocs))
	copy(reducerAllocs, resp.Allocs)
}

// Report Map Task Complete
func reportMapDone(jobNum int, client *rpc.Client) {
	req := types.JobDoneRequest{
		WorkerId: myName,
		JobNum:   jobNum,
	}
	resp := ""
	client.Call("CoordinatorAPI.ReportMapDone", req, &resp)
}

// Report Reduce Task Complete
func reportReduceDone(jobNum int, client *rpc.Client) {
	req := types.JobDoneRequest{
		WorkerId: myName,
		JobNum:   jobNum,
	}
	resp := ""
	client.Call("CoordinatorAPI.ReportReduceDone", req, &resp)
}

func getDataFileNumber(name string) (int, bool) {
	if !strings.HasPrefix(name, "data") {
		return 0, false
	}

	suffix := name[4:]
	if suffix == "" {
		return 0, false
	}

	for _, r := range suffix {
		if r < '0' || r > '9' {
			return 0, false
		}
	}

	var n int
	_, err := fmt.Sscanf(suffix, "%d", &n)
	if err != nil {
		return 0, false
	}

	return n, true
}

func getLocalDataFiles() []int {
	entries, err := os.ReadDir("/")
	if err != nil {
		panic(err)
	}

	files := []int{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if n, ok := getDataFileNumber(entry.Name()); ok {
			files = append(files, n)
		}
	}

	return files
}

func main() {
	myName = os.Args[1]

	// go listenForPeerRPC()

	// time.Sleep(time.Second)

	// client, err := rpc.Dial("tcp", "coordinator:3001") 
	// if err != nil {
	// 	panic(err)
	// }
	
	// getReducerAllocs(client)

	// for {
	// 	req := types.TaskRequest{
	// 		WorkerId: myName,
	// 		Files:    getLocalDataFiles(),
	// 	}
	// 	resp := types.TaskResponse{}

	// 	err := client.Call("CoordinatorAPI.GetJob", req, &resp)
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	switch {
	// 	case resp.TaskM != nil:
	// 		mapData(resp.TaskM.JobNum, resp.TaskM.InputFile, resp.TaskM.R, client)

	// 	case resp.TaskR != nil:
	// 		reduceData(resp.TaskR.JobNum)
	// 		reportReduceDone(resp.TaskR.JobNum, client)

	// 	case resp.Done:
	// 		fmt.Println("Shutting down!")
	// 		client.Close()
	// 		return
	// 	}

	// 	time.Sleep(time.Second)
	// }
	fmt.Println(myName)
	fmt.Println("Shutting down!")
	// client.Close()
}