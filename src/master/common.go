package master

const (
	OK = "OK"
)

type Err string

type PingArgs struct {
}

type PingReply struct {
	Err Err
}
