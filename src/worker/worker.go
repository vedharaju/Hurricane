package worker

import "fmt"
import "net"
import "net/rpc"
import "sync"
import "log"

type Worker struct {
	mu sync.Mutex
	l  net.Listener
	me int
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

func StartServer(servers []string, me int) *Worker {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// gob.Register()

	worker := new(Worker)
	worker.me = me

	rpcs := rpc.NewServer()
	rpcs.Register(worker)

	// os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	worker.l = l

	go func() {
		if conn, err := worker.l.Accept(); err == nil {
			go rpcs.ServeConn(conn)
		} else {
			fmt.Printf("Worker(%v) accept: %v\n", me, err.Error())
			worker.kill()
		}
	}()

	return worker
}
