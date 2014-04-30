package master

import (
	"github.com/eaigner/hood"
)

// Instantiate a workflow batch based on a worflow. This includes creating RDDs
// for each Protojob, and connecting them with RddEdges.  The caller of this
// function should wrap it in a trasnaction
func (workflow *Workflow) MakeBatch(hd *hood.Hood, start int) *WorkflowBatch {
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
		source_rdd := GetRddByStartTime(hd, workflowEdge.SourceJobId, start-workflowEdge.Delay*workflow.Duration)
		if source_rdd != nil {
			rddEdge := &RddEdge{
				SourceRddId:    int64(source_rdd.Id),
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
