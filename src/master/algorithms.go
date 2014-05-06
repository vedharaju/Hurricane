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
	for _, protojob := range workflow.GetProtojobs(hd) {
		rdd := &Rdd{
			WorkflowBatchId: int64(batch.Id),
			ProtojobId:      int64(protojob.Id),
		}
		saveOrPanic(hd, rdd)
		pjToRdd[int64(protojob.Id)] = int64(rdd.Id)
	}

	// Create edges
	for _, workflowEdge := range workflow.GetWorkflowEdges(hd) {
		// Source rdd might be delayed
		if workflowEdge.Delay > 0 {
			source_rdd := GetRddByStartTime(hd, workflowEdge.SourceJobId, start-int64(workflowEdge.Delay)*workflow.Duration)
			if source_rdd != nil {
				rddEdge := &RddEdge{
					SourceRddId:    int64(source_rdd.Id),
					DestRddId:      pjToRdd[workflowEdge.DestJobId],
					WorkflowEdgeId: int64(workflowEdge.Id),
				}
				saveOrPanic(hd, rddEdge)
			}
		} else {
			rddEdge := &RddEdge{
				SourceRddId:    pjToRdd[workflowEdge.SourceJobId],
				DestRddId:      pjToRdd[workflowEdge.DestJobId],
				WorkflowEdgeId: int64(workflowEdge.Id),
			}
			saveOrPanic(hd, rddEdge)
		}
	}

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
func (rdd *Rdd) CreateSegments(hd *hood.Hood) []*Segment {
	pj := rdd.GetProtojob(hd)
	segments := make([]*Segment, pj.NumSegments)
	for i := 0; i < pj.NumSegments; i++ {
		s := &Segment{
			RddId:    int64(rdd.Id),
			WorkerId: 0,
			Status:   0,
			Index:    i,
		}
		saveOrPanic(hd, s)
		segments[i] = s
	}
	return segments
}

// Calculate the input segments for a given segment
func (segment *Segment) CalculateInputSegments(hd *hood.Hood) []*client.SegmentInput {
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

	output := make([]*client.SegmentInput, 0)

	for _, inputRddEdge := range inputRddEdges {
		inputWorkflowEdge := inputWorkflowEdgeMap[inputRddEdge.WorkflowEdgeId]
		sourceRdd := sourceRddMap[inputRddEdge.SourceRddId]
		sourcePj := sourceProtojobMap[sourceRdd.ProtojobId]
		sourceSegments := sourceRdd.GetSegments(hd)
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
					input := &client.SegmentInput{
						SegmentId:      int64(sourceSegment.Id),
						PartitionIndex: i,
						WorkerUrl:      workerMap[sourceSegment.WorkerId].Url,
						Index:          inputWorkflowEdge.Index,
					}
					output = append(output, input)
				}
			}
		}
	}

	return output
}
