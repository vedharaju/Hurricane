package worker

//import "net/rpc"
//import "fmt"
import "sync"

const (
	OK = "OK"
)

type Err string

type PingArgs struct {
}

type PingReply struct {
	Err Err
}

type Clerk struct {
	servers []string
	mu      sync.Mutex
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	return ck
}

/*
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
	c, errx := rpc.Dial("unix", srv)
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

func (ck *Clerk) Ping(server string) bool {
	args := PingArgs{}
	reply := PingReply{}
	ok := call(server, "Worker.Ping", args, &reply)
	if ok && reply.Err == OK {
		return true
	}
	return false
}*/
