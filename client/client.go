package main

import (
	"distributed_search_engine/types"
	"embed"
	"fmt"
	"math/rand"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

var p = 100
var requestNum = 1000
var wg = sync.WaitGroup{}
var mu = sync.Mutex{}

//go:embed words_alpha.txt
var wordsFile embed.FS

var words []string
var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

func loadWords() {
	data, err := wordsFile.ReadFile("words_alpha.txt")
	if err != nil {
		panic(err)
	}

	for _, w := range strings.Split(string(data), "\n") {
		w = strings.TrimSpace(strings.ToLower(w))
		if w != "" {
			words = append(words, w)
		}
	}
}

func randomWord() string {
	return words[rng.Intn(len(words))]
}

func queryServer(client *rpc.Client, keyword string){
	req := types.RedirectRequest{
		Keyword: keyword,
	}

	resp := types.RedirectResponse{}

	err := client.Call("CoordinatorAPI.RedirectClient", req, &resp)
	if err != nil {
		panic(err)
	}

	queryReq := types.SearchRequest{
		Keyword: keyword,
		OutputFile: resp.OutputFile,
	}

	queryResp := types.SearchResponse{}

	worker, err := rpc.Dial("tcp", resp.Address)
	if err != nil {
		panic(err)
	}
	defer worker.Close()

	queryErr := worker.Call("WorkerAPI.ServeQuery", queryReq, &queryResp)
	if queryErr != nil {
		panic(queryErr)
	}
	fmt.Println("searching:", keyword)
	fmt.Println(queryResp)
}


func queryThread(client *rpc.Client) {
	for {
		mu.Lock()
		if requestNum <= 0 {
			mu.Unlock()
			break
		} else {
			requestNum--
			mu.Unlock()
		}

		keyword := randomWord()

		queryServer(client, keyword)
	}
	wg.Done()
}

func main(){
	loadWords()

	client, err := rpc.Dial("tcp", "coordinator:3001") 
	if err != nil {
		panic(err)
	}
	defer client.Close()

	start := time.Now()

	/* Spawn in the threads */
	for i := 0; i < p; i++ {
		wg.Add(1)
		go queryThread(client)
	}
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Println("All queries satsified in ", elapsed)
}