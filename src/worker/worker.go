package worker

import "fmt"
import "net"
import "net/rpc"
import "sync"
import "log"
import "strings"
import "client"
import "time"

type Worker struct {
	mu     sync.Mutex
	l      net.Listener
	master *client.MasterClerk

	batches  map[int][]int64
	segments *LRU

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
	if args.WorkerId != w.master.GetId() {
		segment := w.LocalGetSegment(args.SegmentId)
		if segment != nil {
			reply.Tuples = segment.Partitions[args.PartitionIndex]
			reply.Err = client.OK
		} else {
			reply.Err = client.SEGMENT_NOT_FOUND
		}
	} else {
		// The request is old, and this worker has died, rebooted,
		// and re-registered
		reply.Err = client.DEAD_SEGMENT
	}
	return nil
}

func (w *Worker) GetSegment(args *GetSegmentArgs, reply *GetSegmentReply) error {
	fmt.Println("GET SEGMENT RPC")
	if args.WorkerId != w.master.GetId() {
		segment := w.LocalGetSegment(args.SegmentId)
		if segment != nil {
			reply.Segment = segment
			reply.Err = client.OK
		} else {
			reply.Err = client.SEGMENT_NOT_FOUND
		}
	} else {
		// The request is old, and this worker has died, rebooted,
		// and re-registered
		reply.Err = client.DEAD_SEGMENT
	}
	return nil
}

func (w *Worker) LocalGetSegment(segmentId int64) *Segment {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segments.Get(segmentId)
}

func (w *Worker) LocalPutSegment(segmentId int64, segment *Segment) {
	w.mu.Lock()
	defer w.mu.Unlock()
	segment.Id = segmentId
	w.segments.Insert(segmentId, segment)
}

func (w *Worker) CopySegment(args *client.CopySegmentArgs, reply *client.CopySegmentReply) error {
	fmt.Println("copying segment", args)
	if w.LocalGetSegment(args.SegmentId) != nil {
		// this should never happen during normal operaiton (though it might
		// happen during the master recovery procedure)
		fmt.Println("already have segment, overwriting...")
	}
	fmt.Println("fetching segment", args.SegmentId)
	clerk := MakeWorkerInternalClerk(args.WorkerUrl)
	args2 := GetSegmentArgs{SegmentId: args.SegmentId}
	reply2 := clerk.GetSegment(&args2, 3)
	if reply2 != nil {
		if reply2.Err == client.OK {
			fmt.Println("fetched segment", args.SegmentId)
			w.LocalPutSegment(args.SegmentId, reply2.Segment)
			reply.Err = client.OK
		} else {
			reply.Err = reply2.Err
			reply.WorkerId = args.WorkerId
			fmt.Println(reply.Err)
			return nil
		}
	} else {
		reply.Err = client.DEAD_SEGMENT
		reply.WorkerId = args.WorkerId
		fmt.Println(reply.Err)
		return nil
	}
	return nil
}

func (w *Worker) ExecTask(args *client.ExecArgs, reply *client.ExecReply) error {
	var inputTuples []Tuple
	fmt.Println("executing task", args)
	for _, segment := range args.Segments {
		localSegment := w.LocalGetSegment(segment.SegmentId)
		// fetch the segment if it is not already stored locally
		if localSegment == nil {
			fmt.Println("fetching tuples", segment)
			clerk := MakeWorkerInternalClerk(segment.WorkerUrl)
			args2 := GetTuplesArgs{SegmentId: segment.SegmentId, PartitionIndex: segment.PartitionIndex}
			reply2 := clerk.GetTuples(&args2, 3)
			if reply2 != nil {
				if reply2.Err == client.OK {
					fmt.Println("fetched tuples", len(reply2.Tuples))
					inputTuples = append(inputTuples, reply2.Tuples...)
				} else {
					reply.Err = reply2.Err
					reply.WorkerId = segment.WorkerId
					fmt.Println(reply.Err)
					return nil
				}
			} else {
				reply.Err = client.DEAD_SEGMENT
				reply.WorkerId = segment.WorkerId
				fmt.Println(reply.Err)
				return nil
			}
		} else {
			// use the locally stored copy
			inputTuples = append(inputTuples, localSegment.Partitions[segment.PartitionIndex]...)
		}
	}

	fmt.Println("running udf")
	start := time.Now()
	outputTuples := runUDF(args.Command, inputTuples)
	end := time.Now()
	fmt.Println("duration:", end.Sub(start))
	fmt.Println("got output tuples", len(outputTuples))

	fmt.Println("writing segment")
	segment := MakeSegment(outputTuples, args.Indices, args.Parts)
	w.LocalPutSegment(args.OutputSegmentId, segment)

	fmt.Println("success")
	reply.Err = client.OK
	return nil
}

func (w *Worker) DeleteBatches(args *client.DeleteArgs, reply *client.DeleteReply) error {

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

	// Register the worker to master
	fmt.Println("Registering worker")
	worker.master.Register(true)
	fmt.Println("Registered worker")

	worker.segments = NewLRU(worker.max_segments, worker.master.GetId())

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

	go func() {
		for {
			// Continuously ping the master so that the master is notified
			// when a network partition is resolved.
			reply := worker.master.Ping(true)
			if reply == client.RESET {
				panic("ping rejected by master")
			}
			time.Sleep(1 * time.Second)
		}
	}()

	return worker
}
