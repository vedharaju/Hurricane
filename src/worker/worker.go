package worker

import "fmt"
import "net"
import "net/rpc"
import "sync"
import "log"
import "master"
import "strings"

type Worker struct {
	mu     sync.Mutex
	l      net.Listener
	master string
}

func (w *Worker) Ping(args *PingArgs, reply *PingReply) error {
	reply.Err = OK

	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (w *Worker) kill() {
	w.l.Close()
}

func (w *Worker) GetTuples(args *GetTuplesArgs, reply *GetTuplesReply) {

}

func (w *Worker) ExecTask(args *ExecArgs, reply *ExecReply) error {
	var inputTuples []Tuple
	for segment := range args.Segments {
		args := GetTuplesArgs{SegmentId: segment.SegmentId, PartitionIndex: segment.PartitionIndex, Index: segment.Index}
		reply := GetTuplesReply{}
		ok := call(segment.WorkerUrl, "Worker.GetTuples", &args, &reply)
		if ok && reply.Err == OK {
			inputTuples.append(reply.Tuples)
		}
	}

	outputTuples := runUDF(args.Command, inputTuples)

	for {
		ok := call(worker.master, "Master.execTaskSuccess", &args, &reply)
		if ok && reply.Err == OK {
			break
		}
		// TODO: Sleep here for some time
	}

}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string, args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("tcp", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func StartServer(hostname string, masterhost string) *Worker {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// gob.Register()

	fmt.Println("Starting worker")
	worker := new(Worker)
	worker.master = masterhost

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
		if conn, err := worker.l.Accept(); err == nil {
			go rpcs.ServeConn(conn)
		} else {
			fmt.Printf("Worker(%s) accept: %v\n", hostname, err.Error())
			worker.kill()
		}
	}()

	// Register the worker to master
	fmt.Println("Registered worker")
	ok := false
	for !ok {
		args := master.RegisterArgs{Me: hostname}
		reply := master.RegisterReply{}
		ok = call(worker.master, "Master.Register", args, &reply)
		if ok && reply.Err != OK {
			ok = false
		}
	}
	fmt.Println("Registered worker")

	return worker
}
