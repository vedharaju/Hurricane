package master

import "net/rpc"
import "fmt"
import "sync"
import "time"

type Clerk struct {
	mu sync.Mutex

	// (host:port) information
	master string
	me     string
	id     int64
}

func MakeClerk(me string, master string) *Clerk {
	ck := new(Clerk)

	ck.master = master
	ck.me = me

	return ck
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

// If retry=false, return NO_RESPONSE on failure
func (ck *Clerk) Ping(retry bool) Err {
	// id should be at least 1 if registration was successful
	if ck.id <= 0 {
		panic("Cannot ping before registering")
	}

	to := 10 * time.Millisecond
	for {
		args := PingArgs{Id: ck.id}
		reply := PingReply{}
		ok := call(ck.master, "Master.Ping", &args, &reply)
		if ok {
			// either OK or RESET
			return reply.Err
		}

		if retry == false {
			return NO_RESPONSE
		}

		// exponential backoff
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

// If retry=false, return 0 upon failure
func (ck *Clerk) Register(retry bool) int64 {
	to := 50 * time.Millisecond
	for {
		args := RegisterArgs{Me: ck.me}
		reply := RegisterReply{}
		ok := call(ck.master, "Master.Register", &args, &reply)

		if ok && reply.Err == OK {
			ck.id = reply.Id
			return reply.Id
		}

		if retry == false {
			return 0
		}

		// exponential backoff
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}
