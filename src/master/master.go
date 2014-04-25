package master

import "fmt"
import "time"
import "net"
import "net/rpc"
import "sync"
import "log"
import "github.com/eaigner/hood"
import "strings"

type Master struct {
	mu sync.Mutex
	l  net.Listener
	me int

	// Map of workers to latest ping time
	workers map[string]time.Time
}

//
// server Ping RPC handler.
//
func (m *Master) Ping(args *PingArgs, reply *PingReply) error {
	m.workers[args.Me] = time.Now()

	reply.Err = OK

	return nil
}

//
// server Register RPC handler.
//
func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
        fmt.Println("Registering", args.Me)
	m.workers[args.Me] = time.Now()

	reply.Err = OK

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered.
//
func (m *Master) tick() {
	// Clean dead servers
	for k, v := range m.workers {
			if PingInterval*DeadPings < time.Since(v) {
			delete(m.workers, k)
		}
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (m *Master) kill() {
	m.l.Close()
}

func StartServer(hostname string, hd *hood.Hood) *Master {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// gob.Register()

	master := new(Master)

	master.workers = make(map[string]time.Time)

	rpcs := rpc.NewServer()
	rpcs.Register(master)

	// ignore the domain name: listen on all urls
	splitName := strings.Split(hostname, ":")
	l, e := net.Listen("tcp", ":"+splitName[1])
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

	// create a thread to call tick() periodically.
	go func() {
		master.tick()
		time.Sleep(PingInterval)
	}()

	return master
}
