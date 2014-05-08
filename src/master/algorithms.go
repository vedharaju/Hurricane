package master

import (
	"client"
	"github.com/eaigner/hood"
)

// Instantiate a workflow batch based on a worflow. This includes creating RDDs
// for each Protojob, and connecting them with RddEdges.  The caller of this
// function should wrap it in a trasnaction
func (workflow *Workflow) MakeBatch(hd *hood.Hood, start int64) *WorkflowBatch {
	// Create workflowBatch object
	batch := &WorkflowBatch{
		WorkflowId: int64(workflow.Id),
		StartTime:  start,
	}
	saveOrPanic(hd, batch)

	// Create rdd objects
	pjToRdd := make(map[int64]int64)
	rdds := make([]Rdd, 0)
	for _, protojob := range workflow.GetProtojobs(hd) {
		rdd := Rdd{
			WorkflowBatchId: int64(batch.Id),
			ProtojobId:      int64(protojob.Id),
		}
		rdds = append(rdds, rdd)
	}
	saveAllOrPanic(hd, &rdds)
	for _, rdd := range rdds {
		pjToRdd[rdd.ProtojobId] = int64(rdd.Id)
	}

	// Create edges
	edges := make([]RddEdge, 0)
	for _, workflowEdge := range workflow.GetWorkflowEdges(hd) {
		// Source rdd might be delayed
		if workflowEdge.Delay > 0 {
			source_rdd := GetRddByStartTime(hd, workflowEdge.SourceJobId, start-int64(workflowEdge.Delay)*workflow.Duration)
			if source_rdd != nil {
				rddEdge := RddEdge{
					SourceRddId:    int64(source_rdd.Id),
					DestRddId:      pjToRdd[workflowEdge.DestJobId],
					WorkflowEdgeId: int64(workflowEdge.Id),
				}
				edges = append(edges, rddEdge)
			}
		} else {
			rddEdge := RddEdge{
				SourceRddId:    pjToRdd[workflowEdge.SourceJobId],
				DestRddId:      pjToRdd[workflowEdge.DestJobId],
				WorkflowEdgeId: int64(workflowEdge.Id),
			}
			edges = append(edges, rddEdge)
		}
	}
	saveAllOrPanic(hd, &edges)

	return batch
}

// Find the first Rdds that should be executed in a given batch.
// Source Rdds don't have any inputs from other Rdds in the current batch.
func (workflowBatch *WorkflowBatch) FindSourceRdds(hd *hood.Hood) []*Rdd {
	edges := workflowBatch.GetNonDelayRddEdges(hd)
	rdds := workflowBatch.GetRdds(hd)

	dests := make(map[int64]bool)
	for _, edge := range edges {
		dests[edge.DestRddId] = true
	}

	output := make([]*Rdd, 0)
	for _, rdd := range rdds {
		if dests[int64(rdd.Id)] == false {
			output = append(output, rdd)
		}
	}

	return output
}

// Create segments for a given RDD, but don't assign workers
func (rdd *Rdd) CreateSegments(hd *hood.Hood) ([]*Segment, []*SegmentCopy) {
	pj := rdd.GetProtojob(hd)
	segments := make([]*Segment, pj.NumSegments)
	segmentCopies := make([]*SegmentCopy, pj.NumSegments*pj.Copies)
	for i := 0; i < pj.NumSegments; i++ {
		s := &Segment{
			RddId:    int64(rdd.Id),
			WorkerId: 0,
			Status:   0,
			Index:    i,
		}
		saveOrPanic(hd, s)
		segments[i] = s
		for j := 0; j < pj.Copies; j++ {
			c := &SegmentCopy{
				SegmentId: int64(s.Id),
				WorkerId:  0,
				Status:    0,
			}
			saveOrPanic(hd, c)
			segmentCopies[i*pj.Copies+j] = c
		}
	}
	return segments, segmentCopies
}

// Calculate the input segments for a given segment
func (segment *Segment) CalculateInputSegments(hd *hood.Hood) ([]*client.SegmentInput, []*Rdd) {
	rdd := segment.GetRdd(hd)
	pj := rdd.GetProtojob(hd)

	inputRddEdges := rdd.GetInputEdges(hd)
	inputRddEdgeMap := make(map[int64]*RddEdge)
	for _, edge := range inputRddEdges {
		inputRddEdgeMap[int64(edge.Id)] = edge
	}

	inputWorkflowEdges := pj.GetInputEdges(hd)
	inputWorkflowEdgeMap := make(map[int64]*WorkflowEdge)
	for _, edge := range inputWorkflowEdges {
		inputWorkflowEdgeMap[int64(edge.Id)] = edge
	}

	sourceProtojobs := pj.GetSourceProtojobs(hd)
	sourceProtojobMap := make(map[int64]*Protojob)
	for _, pj2 := range sourceProtojobs {
		sourceProtojobMap[int64(pj2.Id)] = pj2
	}

	sourceRdds := rdd.GetSourceRdds(hd)
	sourceRddMap := make(map[int64]*Rdd)
	for _, rdd2 := range sourceRdds {
		sourceRddMap[int64(rdd2.Id)] = rdd2
	}

	workers := GetWorkers(hd)
	workerMap := make(map[int64]*Worker)
	for _, worker := range workers {
		workerMap[int64(worker.Id)] = worker
	}

	missingRdds := make([]*Rdd, 0)
	for _, rdd := range sourceRdds {
		if rdd.State != RDD_COMPLETE {
			missingRdds = append(missingRdds, rdd)
		}
	}

	output := make([]*client.SegmentInput, 0)

	for _, inputRddEdge := range inputRddEdges {
		inputWorkflowEdge := inputWorkflowEdgeMap[inputRddEdge.WorkflowEdgeId]
		sourceRdd := sourceRddMap[inputRddEdge.SourceRddId]
		sourcePj := sourceProtojobMap[sourceRdd.ProtojobId]
		sourceSegments := sourceRdd.GetSegments(hd)
		sourceSegmentCopies := sourceRdd.GetSegmentCopies(hd)
		// group the segment copies by segment id
		segmentCopyMap := make(map[int64][]*SegmentCopy)
		for _, cp := range sourceSegmentCopies {
			segmentCopyMap[cp.SegmentId] = append(segmentCopyMap[cp.SegmentId], cp)
		}
		for _, sourceSegment := range sourceSegments {
			for i := 0; i < sourcePj.NumBuckets; i++ {
				// calculate the assignment for the source segment
				var segmentAssignment int
				if pj.IsReduce {
					segmentAssignment = i % pj.NumSegments
				} else {
					segmentAssignment = (sourceSegment.Index*sourcePj.NumBuckets + i) * pj.NumSegments / (sourcePj.NumBuckets * sourcePj.NumSegments)
				}
				// is the source segment assigned to this one?
				if segmentAssignment == segment.Index {
					// find a worker that is alive and contains this segment
					// first check the master replica
					masterWorker := workerMap[sourceSegment.WorkerId]
					var worker *Worker
					if masterWorker.Status != WORKER_ALIVE {
						// check if any of the replicas are alive
						for _, cp := range segmentCopyMap[int64(sourceSegment.Id)] {
							backupWorker := workerMap[cp.WorkerId]
							if backupWorker.Status == WORKER_ALIVE {
								worker = backupWorker
							}
						}
					} else {
						worker = masterWorker
					}
					if worker != nil {
						// if a living worker was found
						input := &client.SegmentInput{
							SegmentId:      int64(sourceSegment.Id),
							PartitionIndex: i,
							WorkerUrl:      worker.Url,
							WorkerId:       int64(worker.Id),
							Index:          inputWorkflowEdge.Index,
						}
						output = append(output, input)
					} else {
						// else, mark this RDD as being incomplete
					}
				}
			}
		}
	}

	return output, missingRdds
}
