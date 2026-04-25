package types

type TaskRequest struct {
	WorkerId string
	Files []int
}

type MapTask struct {
	JobNum int
	InputFile string
	R int
}

type ReduceTask struct {
	JobNum int
}

type TaskResponse struct {
	TaskM *MapTask
	TaskR *ReduceTask
	Done bool
}

type JobDoneRequest struct {
	WorkerId string
	JobNum int
}
