package main

import (
	"distributed_search_engine/types"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
)

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


type PeerAPI struct{}

var myName string
var reducerAllocs []string
var incomingMu sync.Mutex
var sentBatches = make(map[int]int)
var sentBatchesMu sync.Mutex

/* Distributes the given key words across the intermediate files */
func mrHash(key string, r int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff) % r
}

// save incoming mappings to be reduced
// func (p *PeerAPI) ReceiveBatch(args MapResultBatch, response *string) error {
// 	incomingMu.Lock()
// 	incomingReduceData[args.ReduceID] = append(incomingReduceData[args.ReduceID], args.Data...)
// 	incomingMu.Unlock()

// 	*response = "ok"
// 	return nil
// }

// listen for incoming mappings to be reduced
// func listenForPeerRPC() {
// 	peer_api := new(PeerAPI)
// 	rpc.Register(peer_api)

// 	listener, _ := net.Listen("tcp", ":2001")

// 	for {
// 		conn, _ := listener.Accept()
// 		go rpc.ServeConn(conn)
// 	}
// }


func mapData(client *rpc.Client, resp types.TaskResponse) {
	result := make(map[string]bool) // resulting urls
	indexes := make([]ReverseIndex, resp.TaskM.R)

	for i := range indexes{
		indexes[i] = make(ReverseIndex)
	}



	urls := resp.TaskM.Urls
	for _, v := range urls {
		err := fetch(v, indexes, result)
		if err != nil {
			fmt.Println("URL: ", v, " ", err)
		}
	}

	for i := range indexes{
		appendIndex(resp.TaskM.IntermFiles[i], indexes[i])
	}

	doneReq := types.MapDoneRequest{
		Urls: result,
	}
	doneResp := types.TaskResponse{}
	
	err := client.Call("CoordinatorAPI.ReportMapDone", doneReq, &doneResp)
	if err != nil {
		panic(err)
	} 	
}

func main() {
	myName = os.Args[1]

	// go listenForPeerRPC()

	// time.Sleep(time.Second)
	client, err := rpc.Dial("tcp", "coordinator:3001") 
	if err != nil {
		panic(err)
	}

	for {
		fmt.Println("Attempting to call coordinator again!")
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
			fmt.Println("Successfully got a map task")
			mapData(client, resp)

		case resp.TaskR != nil:
			// reduceData(resp.TaskR.JobNum)
			// reportReduceDone(resp.TaskR.JobNum, client)

		case resp.Done:
			fmt.Println("Shutting down!")
			fmt.Println(myName)
			client.Close()
			return
		}

		time.Sleep(time.Second)
	}
}