package master

import (
	"github.com/eaigner/hood"
)

// Instantiate a workflow batch based on a worflow. This includes creating RDDs
// for each Protojob, and connecting them with RddEdges.  The caller of this
// function should wrap it in a trasnaction
func (workflow *Workflow) MakeBatch(hd *hood.Hood, start int, duration int) *WorkflowBatch {
	// Create workflowBatch object
	batch := &WorkflowBatch{
		WorkflowId: int64(workflow.Id),
		StartTime:  start,
		Duration:   duration,
	}
	saveOrPanic(hd, batch)

	// Create rdd objects
	pjToRdd := make(map[int64]int64)
	for _, protojob := range workflow.getProtojobs(hd) {
		rdd := &Rdd{
			WorkflowBatchId: int64(batch.Id),
			ProtojobId:      int64(protojob.Id),
		}
		saveOrPanic(hd, rdd)
		pjToRdd[int64(protojob.Id)] = int64(rdd.Id)
	}

	// Create edges (TODO: also create edges for inter-batch dependencies)
	for _, workflowEdge := range workflow.getWorkflowEdges(hd) {
		rddEdge := &RddEdge{
			SourceRddId: pjToRdd[workflowEdge.SourceJobId],
			DestRddId:   pjToRdd[workflowEdge.DestJobId],
		}
		saveOrPanic(hd, rddEdge)
	}

	return batch
}
