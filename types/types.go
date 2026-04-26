package types

type TaskRequest struct {
	WorkerId string
}

type MapTask struct {
	R int // Number of reduce tasks
	Urls []string // Batch of URLs to crawl
	IntermFiles []string // Names of expected intermediate files
}

type ReduceTask struct {
	/* List of intermediate files that should have been replicated over */
	Files []string

}

type TaskResponse struct {
	TaskM *MapTask
	TaskR *ReduceTask
	Done bool
}

type MapDoneRequest struct {
	Urls map[string]bool
}

type MapDoneResponse struct {
	Ok bool
}

type ReduceDoneRequest struct {
	WorkerId string
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
