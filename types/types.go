package types

type TaskRequest struct {
	WorkerId string
}

type MapTask struct {
	R int // Number of reduce tasks
	Urls []string // Batch of URLs to crawl
	IntermFiles []string // Names of expected intermediate files
	Id int
}

type ReduceTask struct {
	/* List of intermediate files that should have been replicated over */
	Files []string
	ReduceId int

}

type TaskResponse struct {
	TaskM *MapTask
	TaskR *ReduceTask
	Done bool
}

type MapDoneRequest struct {
	Urls map[string]bool
	MapId int
}

type MapDoneResponse struct {
	Ok bool
}

type ReduceDoneRequest struct {
	WorkerId string
	OutputFile string
	ReduceId int
}

type ReduceDoneResponse struct {
	Ok bool
}

type InitTransferRequest struct {
	Address string
	Filename string
}

type InitTransferResponse struct {
	Ok bool
}

type RecieveTransferRequest struct {
	Filename string
	Data []byte
}

type RecieveTransferResponse struct {
	Ok bool
}

type HeartbeatRequest struct {
	WorkerId string
}

type HeartbeatResponse struct {
	Ok bool
}

type SearchRequest struct {
	Keyword string
}

type RedirectResponse struct {
	Address string
}

type SearchResponse struct {
	Urls []string
}
