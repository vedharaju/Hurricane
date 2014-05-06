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
	JOB_COMPLETE = 5
)

type EventType int

type Event struct {
	Type EventType
	Id   int64
}
