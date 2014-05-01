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

// maximum clock error in milliseconds
const TIME_ERROR = 100

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
func (m *Master) numOutstandingEvents() int64 {
	return atomic.LoadInt64(&m.numQueuedEvents)
}

// Add an event to the event queue
func (m *Master) queueEvent(e Event) {
	atomic.AddInt64(&m.numQueuedEvents, 1)
	m.events <- e
}

func (m *Master) execNewBatch(workflowId int64) {
	tx := m.hd.Begin()

	// look up workflow
	workflow := GetWorkflow(tx, workflowId)
	// create new workflowbatch
	lastBatch := workflow.GetLastWorkflowBatch(tx)

	now := int(time.Now().Unix())
	if lastBatch == nil {
		// if no last batch, then create the first batch right now - duration - time_eror
		batch := workflow.MakeBatch(tx, now-workflow.Duration-TIME_ERROR)
		m.launchBatchSourceJobs(batch)
	} else {
		// TODO: figure out what exactly to do if there are multiple
		// batches to catch up on, or if it is not yet time to execute
		// the next job

		// for now, only launch a new batch if the proper time has arrived
		// (eg. the end time of the new batch has definitely passed)
		if now > lastBatch.StartTime+2*workflow.Duration+TIME_ERROR {
			batch := workflow.MakeBatch(tx, lastBatch.StartTime+workflow.Duration)
			m.launchBatchSourceJobs(batch)
		}
	}

	commitOrPanic(tx)
}

func (m *Master) launchBatchSourceJobs(batch *WorkflowBatch) {
	rdds := batch.FindSourceRdds(m.hd)
	for _, rdd := range rdds {
		e := Event{
			Type: LAUNCH_JOB,
			Id:   int64(rdd.Id),
		}
		m.queueEvent(e)
	}
}

func (m *Master) execLaunchTask(segmentId int64) {
	tx := m.hd.Begin()

	segment := GetSegment(tx, segmentId)
	inputs := segment.CalculateInputSegments(tx)

	// TODO: implement the RPC call
	go func() {
		fmt.Println(inputs)
		e := Event{
			Type: TASK_SUCCESS,
			Id:   segmentId,
		}
		m.queueEvent(e)
	}()

	commitOrPanic(tx)
}

func (m *Master) execTaskSuccess(segmentId int64) {
	tx := m.hd.Begin()

	// TODO: must verify that all segments are on living nodes, otherwise
	// must trigger re-computation of RDDs

	segment := GetSegment(tx, segmentId)
	rdd := segment.GetRdd(tx)
	pj := rdd.GetProtojob(tx)

	segment.Status = SEGMENT_COMPLETE
	saveOrPanic(tx, segment)

	numComplete := rdd.GetNumSegmentsComplete(tx)

	if numComplete == pj.NumSegments {
		e := Event{
			Type: JOB_COMPLETE,
			Id:   int64(rdd.Id),
		}
		m.queueEvent(e)
	}

	commitOrPanic(tx)
}

func (m *Master) execTaskFailure(segmentId int64) {
	// TODO: do something more sophisticated on failure to allow
	// for recovery after worker failure (for example, if the task failed
	// because of missing input segments)
	e := Event{
		Type: LAUNCH_TASK,
		Id:   int64(segmentId),
	}
	m.queueEvent(e)
}

func (m *Master) execLaunchJob(rddId int64) {
	tx := m.hd.Begin()

	// TODO: check that all of the input RDDS are available

	rdd := GetRdd(tx, rddId)
	segments := rdd.CreateSegments(tx)
	for _, segment := range segments {
		e := Event{
			Type: LAUNCH_TASK,
			Id:   int64(segment.Id),
		}
		m.queueEvent(e)
	}

	commitOrPanic(tx)
}

func (m *Master) execJobComplete(rddId int64) {
	tx := m.hd.Begin()

	rdd := GetRdd(tx, rddId)
	destRdds := rdd.GetDestRdds(tx)

	// For each destRdd, check whether all of the srcRdds
	// for that destRdd are complete. If so, launch the job
	// for destRdd
	// TODO: this logic will have to be re-written when fault-tolerance
	// is implemented
	for _, destRdd := range destRdds {
		srcRdds := destRdd.GetSourceRdds(tx)
		isComplete := true
		for _, srcRdd := range srcRdds {
			pj := rdd.GetProtojob(tx)
			numComplete := srcRdd.GetNumSegmentsComplete(tx)
			if numComplete != pj.NumSegments {
				isComplete = false
			}
		}
		if isComplete {
			e := Event{
				Type: LAUNCH_JOB,
				Id:   int64(destRdd.Id),
			}
			m.queueEvent(e)
		}
	}

	commitOrPanic(tx)
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
	// TODO: Clean dead servers

	// TODO: Launch new batches
	tx := m.hd.Begin()

	// Don't launch new batches if the event queue is more than half full
	if m.numOutstandingEvents() < MAX_EVENTS/2 {
		workflows := GetWorkflows(tx)
		// It's okay to spam the NEW_BATCH events, since extra ones are ignored
		for _, workflow := range workflows {
			e := Event{
				Type: NEW_BATCH,
				Id:   int64(workflow.Id),
			}
			m.queueEvent(e)
		}
	}

	commitOrPanic(tx)
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
