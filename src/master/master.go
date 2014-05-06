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
import "client"
import "strconv"

const MAX_EVENTS = 10000

// maximum clock error in milliseconds
const TIME_ERROR = 100

type Master struct {
	numQueuedEvents int64
	numAliveWorkers int64
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
func (m *Master) Ping(args *client.PingArgs, reply *client.PingReply) error {
	fmt.Println("Pinging", args.Id)

	tx := m.hd.Begin()
	w := GetWorker(m.hd, args.Id)
	if w.Dead {
		reply.Err = client.RESET
	} else {
		reply.Err = client.OK
	}
	// Timestamp is automatically upated on save
	tx.Save(w)
	commitOrPanic(tx)

	return nil
}

func (m *Master) eventLoop() {
	to := 1
	for {
		if atomic.LoadInt64(&m.numAliveWorkers) > 0 {
			to = 1
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
			}
		} else {
			fmt.Println("sleeping", to)
			time.Sleep(time.Duration(to) * time.Millisecond)
			if to < 1000 {
				to *= 2
			}
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
	fmt.Println("execNewBatch", workflowId)
	tx := m.hd.Begin()

	// look up workflow
	workflow := GetWorkflow(tx, workflowId)
	// create new workflowbatch
	lastBatch := workflow.GetLastWorkflowBatch(tx)

	now := time.Now().UnixNano() / 1000000
	var batch *WorkflowBatch
	if lastBatch == nil {
		// if no last batch, then create the first batch right now - duration - time_eror
		fmt.Println("No last batch")
		batch = workflow.MakeBatch(tx, now-workflow.Duration-TIME_ERROR)
	} else {
		// TODO: figure out what exactly to do if there are multiple
		// batches to catch up on, or if it is not yet time to execute
		// the next job

		// for now, only launch a new batch if the proper time has arrived
		// (eg. the end time of the new batch has definitely passed)
		fmt.Println(now, lastBatch.StartTime)
		if now > lastBatch.StartTime+2*workflow.Duration+TIME_ERROR {
			fmt.Println("add new batch", workflow.Duration)
			batch = workflow.MakeBatch(tx, lastBatch.StartTime+workflow.Duration)
		}
	}
	commitOrPanic(tx)

	if batch != nil {
		m.launchBatchSourceJobs(batch)
	}
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

func parseIndex(s string) []int {
	s2 := strings.Trim(s, "()")
	splits := strings.Split(s2, ",")
	ints := make([]int, 0)
	for _, split := range splits {
		output, err := strconv.Atoi(split)
		if err == nil {
			ints = append(ints, output)
		}
	}
	return ints
}

func preprocessMasterCommand(cmd string, batch *WorkflowBatch, segment *Segment, workflow *Workflow) string {
	cmd = strings.Replace(cmd, "\\I", string(segment.Index), -1)
	cmd = strings.Replace(cmd, "\\S", string(batch.StartTime), -1)
	cmd = strings.Replace(cmd, "\\D", string(workflow.Duration), -1)
	return cmd
}

func (m *Master) execLaunchTask(segmentId int64) {
	fmt.Println("execLaunchTask", segmentId)
	tx := m.hd.Begin()

	segment := GetSegment(tx, segmentId)
	rdd := segment.GetRdd(tx)
	batch := rdd.GetWorkflowBatch(tx)
	pj := rdd.GetProtojob(tx)
	worker := GetRandomAliveWorker(tx)
	workflow := batch.GetWorkflow(tx)
	if worker != nil {
		segment.WorkerId = int64(worker.Id)
	} else {
		segment.WorkerId = 0
	}
	saveOrPanic(tx, segment)
	commitOrPanic(tx)

	if segment.WorkerId != 0 {
		// if a worker was availble, launch the task
		tx = m.hd.Begin()
		inputs := segment.CalculateInputSegments(tx)
		commitOrPanic(tx)

		command := preprocessMasterCommand(pj.Command, batch, segment, workflow)

		args := &client.ExecArgs{
			Command:         command,
			Segments:        inputs,
			OutputSegmentId: int64(segment.Id),
			Indices:         parseIndex(pj.PartitionIndex),
			Parts:           pj.NumBuckets,
		}

		c := client.MakeWorkerClerk(worker.Url)

		go func() {
			reply := c.ExecTask(args, 3)
			if reply != nil {
				if reply.Err == client.OK {
					// task success
					e := Event{
						Type: TASK_SUCCESS,
						Id:   segmentId,
					}
					m.queueEvent(e)
				} else {
					if reply.Err == client.DEAD_SEGMENT {
						fmt.Println(client.DEAD_SEGMENT)
						// task failed due to dead segment host
						e := Event{
							Type: TASK_FAILURE,
							Id:   segmentId,
						}
						m.queueEvent(e)
						panic("this failure case hasn't been implemented yet")
					} else {
						fmt.Println(client.SEGMENT_NOT_FOUND)
						// task failed due to a segment host that forgot an RDD
						e := Event{
							Type: TASK_FAILURE,
							Id:   segmentId,
						}
						m.queueEvent(e)
						panic("this failure case hasn't been implemented yet")
					}
				}
			} else {
				fmt.Println("DEAD_WORKER")
				m.markDeadWorker(worker)
				// Conclude that the worker is dead
				e := Event{
					Type: TASK_FAILURE,
					Id:   segmentId,
				}
				m.queueEvent(e)
			}
		}()
	} else {
		// if no workers are available, just re-queue the task
		fmt.Println("no workers available")
		e := Event{
			Type: LAUNCH_TASK,
			Id:   segmentId,
		}
		m.queueEvent(e)
	}
}

func (m *Master) markDeadWorker(worker *Worker) {
	// TODO: do something intelligent here
	worker.Dead = true
	tx := m.hd.Begin()
	saveOrPanic(tx, worker)
	commitOrPanic(tx)
	m.getNumAliveWorkers()
}

func (m *Master) execTaskSuccess(segmentId int64) {
	fmt.Println("execTaskSuccess", segmentId)
	tx := m.hd.Begin()

	// TODO: must verify that all segments are on living nodes, otherwise
	// must trigger re-computation of RDDs

	segment := GetSegment(tx, segmentId)
	rdd := segment.GetRdd(tx)
	pj := rdd.GetProtojob(tx)

	segment.Status = SEGMENT_COMPLETE
	saveOrPanic(tx, segment)

	numComplete := rdd.GetNumSegmentsComplete(tx, segment)

	if numComplete == pj.NumSegments {
		fmt.Println("Job complete", rdd.Id)
		rdd.State = RDD_COMPLETE
		saveOrPanic(tx, rdd)

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
				if (srcRdd.State != RDD_COMPLETE) && (srcRdd.Id != rdd.Id) {
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
	}

	commitOrPanic(tx)
}

func (m *Master) execTaskFailure(segmentId int64) {
	fmt.Println("execTaskFailure", segmentId)
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
	fmt.Println("execLaunchJob", rddId)
	tx := m.hd.Begin()

	// TODO: check that all of the input RDDS are available

	rdd := GetRdd(tx, rddId)
	segments := rdd.CreateSegments(tx)

	commitOrPanic(tx)

	for _, segment := range segments {
		e := Event{
			Type: LAUNCH_TASK,
			Id:   int64(segment.Id),
		}
		m.queueEvent(e)
	}
}

//
// server Register RPC handler.
//
func (m *Master) Register(args *client.RegisterArgs, reply *client.RegisterReply) error {
	fmt.Println("Registering", args)

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

	m.getNumAliveWorkers()

	reply.Err = client.OK
	reply.Id = int64(newWorker.Id)

	return nil
}

func (m *Master) getNumAliveWorkers() {
	tx := m.hd.Begin()
	atomic.StoreInt64(&m.numAliveWorkers, int64(GetNumAliveWorkers(tx)))
	commitOrPanic(tx)
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
		for {
			master.tick()
			time.Sleep(TickInterval)
		}
	}()

	return master
}
