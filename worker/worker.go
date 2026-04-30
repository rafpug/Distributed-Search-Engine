package main

import (
	"distributed_search_engine/types"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"golang.org/x/net/html"
)

const heartbeatTime = 10 * time.Second

type ReverseIndex map[string]map[string]bool

var stopWords = map[string]bool {
	"a": true,
	"the": true,
	"is": true,
	"and": true,
	"of": true,
	"to": true,
	"in": true,
}

var regex = regexp.MustCompile("[a-z0-9]+")

func filterStopWords(words []string) []string {
	var res []string
	for _, w := range words {
		if !stopWords[w] {
			res = append(res, w)
		}
	}
	return res
}

func tokenize(text string) []string {
	text = strings.ToLower(text)
	res := regex.FindAllString(text, -1)
	return filterStopWords(res)
}

func resolveURL(baseStr string, refStr string) (string, error) {
	base, err := url.Parse(baseStr)
	if err != nil {
		return "", err
	}

	ref, err := url.Parse(refStr)
	if err != nil {
		return "", err
	}

	return base.ResolveReference(ref).String(), nil
}

func processHTML(baseURL string, n *html.Node, indexes []ReverseIndex, result map[string]bool) {
	if n.Type == html.ElementNode && (n.Data == "script" || n.Data == "style") {
		return
	}
	
	if n.Type == html.TextNode {
		text := strings.TrimSpace(n.Data)
		if text != "" {
			for _, word := range tokenize(text) {
				hash := mrHash(word, len(indexes))
				if indexes[hash][word] == nil {
					indexes[hash][word] = make(map[string]bool)
				}
				indexes[hash][word][baseURL] = true
			}
		}
	}

	if n.Type == html.ElementNode && n.Data == "a" {
		for _, attr := range n.Attr {
			if attr.Key == "href" {
				href := strings.TrimSpace(attr.Val)

				if href == "" ||
					strings.HasPrefix(href, "#") ||
					strings.HasPrefix(href, "javascript:") ||
					strings.HasPrefix(href, "mailto:") {
						continue
				}
				
				url, err := resolveURL(baseURL, href)
				if err != nil {
					continue
				}

				result[url] = true
			}
		}
	}

	for doc := n.FirstChild; doc != nil; doc = doc.NextSibling {
		processHTML(baseURL, doc, indexes, result)
	}
}

func fetch(url string, indexes []ReverseIndex, result map[string]bool) error {
	client := &http.Client{
		Timeout: 15 * time.Second,
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("User-Agent", "MyWikiIndexer/1.0 (salcedop)")
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s", resp.Status)
	}

	doc, err := html.Parse(resp.Body)
	if err != nil {
		return err
	}

	processHTML(url, doc, indexes, result)
	return nil
}

func saveIndex(filename string, index ReverseIndex) error {
	fd, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer fd.Close()

	enc := json.NewEncoder(fd)
	enc.SetIndent("", " ")
	return enc.Encode(index)
}

func loadIndex(filename string) (ReverseIndex, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var index ReverseIndex
	err = json.NewDecoder(file).Decode(&index)
	if err != nil {
		return nil, err
	}

	return index, nil
}

func combineIndex(indexes []ReverseIndex) ReverseIndex {
	result := make(ReverseIndex)
	for _, v := range indexes {
		for word, urls := range v {
			for url, _ := range urls {
				if result[word] == nil {
					result[word] = make(map[string]bool)
				}

				result[word][url] = true
			}
		}
	}
	return result
}

func appendIndex(filename string, index ReverseIndex) error {
	oldIndex, err := loadIndex(filename)

	if os.IsNotExist(err) {
		saveIndex(filename, index)
	} else if err != nil {
		return err
	} else {
		newIndex := combineIndex([]ReverseIndex{index, oldIndex})
		saveIndex(filename, newIndex)
	}
	return nil
}


type WorkerAPI struct{}

/* Distributes the given key words across the intermediate files */
func mrHash(key string, r int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff) % r
}

/* Coordinator initiates a file transfer between workers */
func (w *WorkerAPI) InitiateFileTransfer(req types.InitTransferRequest, resp *types.InitTransferResponse) error {
	data, err := os.ReadFile(req.Filename)
	if err != nil {
		resp.Ok = false
		return err
	}

	recieverReq := types.RecieveTransferRequest {
		Filename: req.Filename,
		Data: data,
	}
	recieverResp := types.RecieveTransferResponse{}

	client, err := rpc.Dial("tcp", req.Address)
	if err != nil {
		resp.Ok = false
		return err
	}
	defer client.Close()

	err = client.Call("WorkerAPI.RecieveFileTransfer", recieverReq, &recieverResp)
	resp.Ok = recieverResp.Ok
	return err
}

/* Worker recieves and writes the file locally */
func (w *WorkerAPI) RecieveFileTransfer(req types.RecieveTransferRequest, resp *types.RecieveTransferResponse) error {
	err := os.WriteFile(req.Filename, req.Data, 0644)
	if err != nil {
		resp.Ok = false
		return err
	}
	resp.Ok = true
	return nil
}

// listen for incoming files that need to be replicated
func listenForWorkerRPC() {
	worker_api := new(WorkerAPI)
	rpc.Register(worker_api)

	listener, _ := net.Listen("tcp", ":2001")

	for {
		conn, _ := listener.Accept()
		go rpc.ServeConn(conn)
	}
}

func reduceData(resp *types.ReduceTask) {
	interm_outputs := []ReverseIndex{}
	for _, file := range resp.Files{
		index, err := loadIndex(file)
		if err != nil {
			panic(err)
		}

		interm_outputs = append(interm_outputs, index)
	}
	output := combineIndex(interm_outputs)
	fileName := fmt.Sprintf("output-%d", resp.ReduceId)
	saveIndex(fileName, output)
}

func reduceDone(client *rpc.Client, workerId string, outId int,) {
	outFile := fmt.Sprintf("output-%d", outId)
	doneReq := types.ReduceDoneRequest{
		WorkerId: workerId,
		OutputFile: outFile,
		ReduceId: outId,
	}
	doneResp := types.ReduceDoneResponse{
		Ok: true,
	}

	err := client.Call("CoordinatorAPI.ReportReduceDone", doneReq, &doneResp)
	if err != nil {
		panic(err)
	}
}

func mapData(client *rpc.Client, resp types.TaskResponse) {
	result := make(map[string]bool) // stores new resulting urls
	indexes := make([]ReverseIndex, resp.TaskM.R)

	for i := range indexes{
		indexes[i] = make(ReverseIndex)
	}



	urls := resp.TaskM.Urls
	for _, v := range urls {
		err := fetch(v, indexes, result)
		if err != nil {
			// fmt.Println("URL: ", v, " ", err)
		}
	}

	for i := range indexes{
		appendIndex(resp.TaskM.IntermFiles[i], indexes[i])
	}

	doneReq := types.MapDoneRequest{
		Urls: result,
		MapId: resp.TaskM.Id,
	}
	doneResp := types.MapDoneResponse{}
	
	err := client.Call("CoordinatorAPI.ReportMapDone", doneReq, &doneResp)
	if err != nil {
		panic(err)
	} 	
}

func generateHeartbeats(client *rpc.Client, workerId string){
	for {
		req := types.HeartbeatRequest{
			WorkerId: workerId,
		}
		resp := types.HeartbeatResponse{}
		err := client.Call("CoordinatorAPI.RecieveHeartbeat", req, &resp)
		if err != nil {
			panic(err)
		}
		time.Sleep(heartbeatTime)
	}
}

func main() {
	myName := os.Args[1]

	go listenForWorkerRPC()

	// time.Sleep(time.Second)
	client, err := rpc.Dial("tcp", "coordinator:3001") 
	if err != nil {
		panic(err)
	}

	go generateHeartbeats(client, myName)

	for {
		req := types.TaskRequest{
			WorkerId: myName,
		}
		resp := types.TaskResponse{}

		err := client.Call("CoordinatorAPI.GetJob", req, &resp)
		if err != nil {
			panic(err)
		}
		

		switch {
		case resp.TaskM != nil:
			mapData(client, resp)

		case resp.TaskR != nil:
			reduceData(resp.TaskR)
			reduceDone(client, myName, resp.TaskR.ReduceId)

		case resp.Done:
			fmt.Println("Shutting down!")
			fmt.Println(myName)
			client.Close()
			return
		}

		time.Sleep(time.Second)
	}
}