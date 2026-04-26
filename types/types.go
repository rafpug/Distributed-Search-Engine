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
	Files map[string][]string // Maps intermediate files to worker addresses
}

type TaskResponse struct {
	TaskM *MapTask
	TaskR *ReduceTask
	Done bool
}

type BatchDoneRequest struct {
	WorkerId string
	JobNum int
}

type MapDoneRequest struct {
	Urls map[string]bool
}

type MapDoneResponse struct {
	Ok bool
}
