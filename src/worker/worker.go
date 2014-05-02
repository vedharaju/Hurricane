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

	segments map[int64]*Segment
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
	reply.Tuples = w.segments[args.SegmentId].Partitions[args.PartitionIndex]
	reply.Err = client.OK
	return nil
}

func (w *Worker) ExecTask(args *client.ExecArgs, reply *client.ExecReply) error {
	var inputTuples []Tuple
	fmt.Println("executing task", args)
	for _, segment := range args.Segments {
		fmt.Println("fetching segment", segment)
		args := GetTuplesArgs{SegmentId: segment.SegmentId, PartitionIndex: segment.PartitionIndex}
		reply := GetTuplesReply{}
		ok := client.CallRPC(segment.WorkerUrl, "Worker.GetTuples", &args, &reply)
		if ok && reply.Err == client.OK {
			inputTuples = append(inputTuples, reply.Tuples...)
		}
	}

	fmt.Println("running udf")
	outputTuples := runUDF(args.Command, inputTuples)

	fmt.Println("writing segment")
	w.segments[args.OutputSegmentId] = MakeSegment(outputTuples, args.Indices, args.Parts)

	fmt.Println("success")
	reply.Err = client.OK
	return nil
}

func StartServer(hostname string, masterhost string) *Worker {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// gob.Register()

	fmt.Println("Starting worker")
	worker := new(Worker)
	worker.master = client.MakeMasterClerk(hostname, masterhost)
	worker.segments = make(map[int64]*Segment)

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
