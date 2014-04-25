package master

import "time"

const (
	OK = "OK"

	// workers should send a Ping RPC this often,
	// to tell the master that the worker is alive.
	PingInterval = time.Millisecond * 100

	// the master will declare a worker dead if it misses
	// this many Ping RPCs in a row.
	DeadPings = 5
)

type Err string

type RegisterArgs struct {
	Me string
}

type RegisterReply struct {
	Err Err
}

type PingArgs struct {
	Me string
}

type PingReply struct {
	Err Err
}
