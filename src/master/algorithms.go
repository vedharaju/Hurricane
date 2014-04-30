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
