package worker

import "sync"
import "master/algorithms"

const (
	OK = "OK"
)

type Err string

type PingArgs struct {
}

type PingReply struct {
	Err Err
}

type GetTuplesArgs struct {
	SegmentId      int64
	PartitionIndex int
}

type GetTuplesReply struct {
	Tuples []Tuple
}

type ExecArgs struct {
	Command         string
	Segments        []algorithms.SegmentInput
	OutputSegmentId int64
	Indices         []int
	Parts           int
}

type ExecReply struct {
}
