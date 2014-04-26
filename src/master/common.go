package master

import "time"

const (
	OK          = "OK"
	RESET       = "RESET"
	NO_RESPONSE = "NO_RESPONSE"

	// workers should send a Ping RPC this often,
	// to tell the master that the worker is alive.
	TickInterval = time.Millisecond * 100

	// the master will declare a worker dead if it misses
	// this many Ping RPCs in a row.
	DeadPings = 5

	NEW_BATCH    = 0
	LAUNCH_TASK  = 1
	TASK_SUCCESS = 2
	TASK_FAILURE = 3
	LAUNCH_JOB   = 4
	JOB_COMPLETE = 5
)

type Err string
type EventType int

type RegisterArgs struct {
	Me string
}

type RegisterReply struct {
	Id  int64
	Err Err
}

type PingArgs struct {
	Id int64
}

type PingReply struct {
	Err Err
}

type Event struct {
	Type EventType
	Id   int64
}
