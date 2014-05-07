package worker

import "fmt"
import "net"
import "net/rpc"
import "sync"
import "log"
import "strings"
import "client"

type Worker struct {
	mu     sync.Mutex
	l      net.Listener
	master *client.MasterClerk

        batches map[int][]int64
	segments map[int64]*Segment

        max_segments int
}

func (w *Worker) Ping(args *client.PingArgs, reply *client.PingReply) error {
	reply.Err = client.OK

	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (w *Worker) kill() {
	w.l.Close()
}

func (w *Worker) GetTuples(args *GetTuplesArgs, reply *GetTuplesReply) error {
	fmt.Println("GET TUPLES RPC")
	segment := w.segments[args.SegmentId]
	if segment != nil {
		reply.Tuples = segment.Partitions[args.PartitionIndex]
		reply.Err = client.OK
	} else {
		reply.Err = client.SEGMENT_NOT_FOUND
	}
	return nil
}

func (w *Worker) GetSegment(args *GetSegmentArgs, reply *GetSegmentReply) error {
	fmt.Println("GET SEGMENT RPC")
	segment := w.segments[args.SegmentId]
	if segment != nil {
		reply.Segment = segment
		reply.Err = client.OK
	} else {
		reply.Err = client.SEGMENT_NOT_FOUND
	}
	return nil
}

func (w *Worker) LocalGetSegment(segmentId int64) *Segment {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segments[segmentId]
}

func (w *Worker) LocalPutSegment(segmentId int64, segment *Segment) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.segments[segmentId] = segment
}

func (w *Worker) CopySegment(args *client.CopySegmentArgs, reply *client.CopySegmentReply) error {
	fmt.Println("copying segment", args)
	if w.LocalGetSegment(args.SegmentId) != nil {
		fmt.Println("already have segment")
	} else {
		fmt.Println("fetching segment", args.SegmentId)
		clerk := MakeWorkerInternalClerk(args.WorkerUrl)
		args2 := GetSegmentArgs{SegmentId: args.SegmentId}
		reply2 := clerk.GetSegment(&args2, 3)
		if reply2 != nil {
			if reply2.Err == client.OK {
				fmt.Println("fetched segment", args.SegmentId)
				w.LocalPutSegment(args.SegmentId, reply2.Segment)
			} else {
				reply.Err = reply2.Err
				fmt.Println(reply.Err)
				return nil
			}
		} else {
			reply.Err = client.DEAD_SEGMENT
			fmt.Println(reply.Err)
			return nil
		}
	}
	return nil
}

func (w *Worker) ExecTask(args *client.ExecArgs, reply *client.ExecReply) error {
	var inputTuples []Tuple
	fmt.Println("executing task", args)
	for _, segment := range args.Segments {
		fmt.Println("fetching segment", segment)
		clerk := MakeWorkerInternalClerk(segment.WorkerUrl)
		args2 := GetTuplesArgs{SegmentId: segment.SegmentId, PartitionIndex: segment.PartitionIndex}
		reply2 := clerk.GetTuples(&args2, 3)
		if reply2 != nil {
			if reply2.Err == client.OK {
				fmt.Println("fetched tuples", len(reply2.Tuples))
				inputTuples = append(inputTuples, reply2.Tuples...)
			} else {
				reply.Err = reply2.Err
				fmt.Println(reply.Err)
				return nil
			}
		} else {
			reply.Err = client.DEAD_SEGMENT
			fmt.Println(reply.Err)
			return nil
		}
	}

	fmt.Println("running udf")
	outputTuples := runUDF(args.Command, inputTuples)
	fmt.Println("got output tuples", len(outputTuples))

	fmt.Println("writing segment")
	segment := MakeSegment(outputTuples, args.Indices, args.Parts)
	w.LocalPutSegment(args.OutputSegmentId, segment)

	fmt.Println("success")
	reply.Err = client.OK
	return nil
}

func (w *Worker) DeleteBatches(args *client.DeleteArgs, reply *cient.DeleteReply) error {
    
  return nil
}

func StartServer(hostname string, masterhost string) *Worker {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// gob.Register()

	fmt.Println("Starting worker")
	worker := new(Worker)
	worker.master = client.MakeMasterClerk(hostname, masterhost)
        worker.batches = make(map[int][]int64)
	worker.segments = make(map[int64]*Segment)
        worker.max_segments = 10

	rpcs := rpc.NewServer()
	rpcs.Register(worker)

	// ignore the domain name: listen on all urls
	splitName := strings.Split(hostname, ":")
	l, e := net.Listen("tcp", ":"+splitName[1])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	worker.l = l

	go func() {
		for {
			if conn, err := worker.l.Accept(); err == nil {
				go rpcs.ServeConn(conn)
			} else {
				fmt.Printf("Worker(%s) accept: %v\n", hostname, err.Error())
				worker.kill()
			}
		}
	}()

	// Register the worker to master
	fmt.Println("Registering worker")
	worker.master.Register(true)
	fmt.Println("Registered worker")

	return worker
}
