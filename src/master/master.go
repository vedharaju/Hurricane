package master

import "fmt"
import "net"
import "net/rpc"
import "sync"
import "log"

type Master struct {
	mu sync.Mutex
	l  net.Listener
	me int
}

func (m *Master) Ping(args *PingArgs, reply *PingReply) error {
	reply.Err = OK

	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (m *Master) kill() {
	m.l.Close()
}

func StartServer(server string) *Master {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// gob.Register()

	master := new(Master)

	rpcs := rpc.NewServer()
	rpcs.Register(master)

	// os.Remove(servers[me])
	l, e := net.Listen("unix", server)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	master.l = l

	go func() {
		if conn, err := master.l.Accept(); err == nil {
			go rpcs.ServeConn(conn)
		} else {
			fmt.Printf("Master() accept: %v\n", err.Error())
			master.kill()
		}
	}()

	return master
}
