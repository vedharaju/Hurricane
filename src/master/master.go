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
	hd *hood.Hood
}

func commitOrPanic(tx *hood.Hood) {
	err := tx.Commit()
	if err != nil {
		panic(err)
	}
}

//
// server Ping RPC handler.
//
func (m *Master) Ping(args *PingArgs, reply *PingReply) error {
	fmt.Println("Pinging", args.Id)

	w := GetWorker(m.hd, args.Id)
	if w.Dead {
		reply.Err = RESET
	} else {
		reply.Err = OK
	}

	return nil
}

//
// server Register RPC handler.
//
func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	fmt.Println("Registering", args.Me)

	tx := m.hd.Begin()
	existingWorkers := GetWorkersAtAddress(tx, args.Me)
	for _, w := range existingWorkers {
		w.Dead = true
		tx.Save(w)
	}
	newWorker := Worker{
		Url: args.Me,
	}
	tx.Save(&newWorker)
	commitOrPanic(tx)

	reply.Err = OK
	reply.Id = int64(newWorker.Id)

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered.
//
func (m *Master) tick() {
	// Clean dead servers
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

	master.hd = hd

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
		for {
			if conn, err := master.l.Accept(); err == nil {
				go rpcs.ServeConn(conn)
			} else {
				fmt.Printf("Master() accept: %v\n", err.Error())
				master.kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		master.tick()
		time.Sleep(PingInterval)
	}()

	return master
}
