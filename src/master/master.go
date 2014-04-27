package master

import "fmt"
import "time"
import "net"
import "net/rpc"
import "sync"
import "sync/atomic"
import "log"
import "github.com/eaigner/hood"
import "strings"

const MAX_EVENTS = 10000

type Master struct {
	numQueuedEvents int64
	mu              sync.Mutex
	l               net.Listener
	me              int
	hd              *hood.Hood

	events chan (Event)
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

	tx := m.hd.Begin()
	w := GetWorker(m.hd, args.Id)
	if w.Dead {
		reply.Err = RESET
	} else {
		reply.Err = OK
	}
	// Timestamp is automatically upated on save
	tx.Save(w)
	commitOrPanic(tx)

	return nil
}

func (m *Master) eventLoop() {
	for {
		e := <-m.events
		atomic.AddInt64(&m.numQueuedEvents, -1)
		switch e.Type {
		case NEW_BATCH:
			m.execNewBatch(e.Id)
		case LAUNCH_TASK:
			m.execLaunchTask(e.Id)
		case TASK_SUCCESS:
			m.execTaskSuccess(e.Id)
		case TASK_FAILURE:
			m.execTaskFailure(e.Id)
		case LAUNCH_JOB:
			m.execLaunchJob(e.Id)
		case JOB_COMPLETE:
			m.execJobComplete(e.Id)
		}
	}
}

// Return the number of events in the event queue.
// Should only be called from the EventLoop thread
func (m *Master) numOutstandingEvents(e Event) int64 {
	return atomic.LoadInt64(&m.numQueuedEvents)
}

// Add an event to the event queue
func (m *Master) queueEvent(e Event) {
	atomic.AddInt64(&m.numQueuedEvents, 1)
	m.events <- e
}

func (m *Master) execNewBatch(workflowId int64) {
	// look up workflow
	// create new workflowbatch
		// generate RDDs for each job
	// queue launch job events for first jobs
}

func (m *Master) execLaunchTask(segmentId int64) {
	// rpc to worker to run task

	// if complete
		// queue task complete event
	// else
		// queue task failed event
}

func (m *Master) execTaskSuccess(segmentId int64) {
	// if last task in job
		// queue job complete event
}

func (m *Master) execTaskFailure(segmentId int64) {
	// queue event to launch job for failed task
}

func (m *Master) execLaunchJob(rddId int64) {
	// RUN JOB YAY!!!

	// Look up RDD related RDD Edges
		// Need to send to Worker:
			// Segments (based on whether they contain a relevant partition)
			// along with address of Worker, parent RDD

	// launch tasks with appropriate data
}

func (m *Master) execJobComplete(rddId int64) {
	// if another job in workflow batch
		// queue next task if RDDs available
	// else
		// finish
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
// tick() is called once per TickInterval; it should notice
// if servers have died or recovered.
//
// Additionally, it should trigger newBatch events when necessary.
//
// Should not start a new batch if the numOutstandingEvents is close
// to overflowing the event queue (because that would deadlock the program).
// Instead, crash gracefully.
//
func (m *Master) tick() {
	// Clean dead servers

	// Launch new batches if necessary
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
	master.events = make(chan (Event), MAX_EVENTS)

	rpcs := rpc.NewServer()
	rpcs.Register(master)

	// ignore the domain name: listen on all urls
	splitName := strings.Split(hostname, ":")
	l, e := net.Listen("tcp", ":"+splitName[1])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	master.l = l

	// start event loop
	go master.eventLoop()

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
		time.Sleep(TickInterval)
	}()

	return master
}
