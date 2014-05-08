package master

import "time"

const (
	// workers should send a Ping RPC this often,
	// to tell the master that the worker is alive.
	TickInterval = time.Millisecond * 1000

	// the master will declare a worker missing if it misses
	// this many Ping RPCs in a row.
	MissingPings = 5

	// declare a worker dead after this many missed pings
	DeadPings = 50

	NEW_BATCH    = 0
	LAUNCH_TASK  = 1
	TASK_SUCCESS = 2
	TASK_FAILURE = 3
	LAUNCH_JOB   = 4
	COPY_SUCCESS = 5
	COPY_FAILURE = 6
	LAUNCH_COPY  = 7

	FAILURE_DEAD_WORKER     = 0
	FAILURE_DEAD_SEGMENT    = 1
	FAILURE_MISSING_SEGMENT = 2
)

type EventType int
type FailureType int

type Event struct {
	Type EventType
	Id   int64
	Data interface{}
}

type FailureData struct {
	Type     FailureType
	WorkerId int64
}
