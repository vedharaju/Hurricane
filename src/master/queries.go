package master

import (
	"github.com/eaigner/hood"
)

func (rdd *Rdd) getSegments(tx *hood.Hood) []*Segment {
	var results []Segment
	err := tx.Where("rdd_id", "=", rdd.Id).Find(&results)
	if err != nil {
		panic(err)
	}

	// Should return pointers to the result objects so that
	// they can be mutated
	pointerResults := make([]*Segment, len(results))
	for i := range results {
		pointerResults[i] = &results[i]
	}

	return pointerResults
}

func (workflow *Workflow) getProtojobs(tx *hood.Hood) []*Protojob {
	var results []Protojob
	err := tx.Where("workflow_id", "=", workflow.Id).Find(&results)
	if err != nil {
		panic(err)
	}

	// Should return pointers to the result objects so that
	// they can be mutated
	pointerResults := make([]*Protojob, len(results))
	for i := range results {
		pointerResults[i] = &results[i]
	}

	return pointerResults
}

// Select all workflow edges whose dest_protojob is in the given
// workflow (this also imlies that the source_protojob is in the workflow)
func (workflow *Workflow) getWorkflowEdges(tx *hood.Hood) []*WorkflowEdge {
	var results []WorkflowEdge
	err := tx.FindSql(&results,
		`select *
    from workflow_edge
    left join protojob dest_job
    on workflow_edge.dest_job_id = dest_job.id
    where dest_job.workflow_id = $1`, workflow.Id)
	if err != nil {
		panic(err)
	}

	// Should return pointers to the result objects so that
	// they can be mutated
	pointerResults := make([]*WorkflowEdge, len(results))
	for i := range results {
		pointerResults[i] = &results[i]
	}

	return pointerResults
}

// Get all Rdd edges whose source or dest Rdd is in the given workflow
// batch. Note that the source and dest Rdds may be produced in different
// batches.
func (workflowBatch *WorkflowBatch) getRddEdges(tx *hood.Hood) []*RddEdge {
	var results []RddEdge
	err := tx.FindSql(&results,
		`select *
    from rdd_edge
    left join rdd source_rdd
    on rdd_edge.source_rdd_id = source_rdd.id
    left join rdd dest_rdd
    on rdd_edge.dest_rdd_id = dest_rdd.id
    where source_rdd.workflow_batch_id = $1
    or dest_rdd.workflow_batch_id = $1`, workflowBatch.Id)
	if err != nil {
		panic(err)
	}

	// Should return pointers to the result objects so that
	// they can be mutated
	pointerResults := make([]*RddEdge, len(results))
	for i := range results {
		pointerResults[i] = &results[i]
	}

	return pointerResults
}

func (workflow *Workflow) getWorkflowBatches(tx *hood.Hood) []*WorkflowBatch {
	var results []WorkflowBatch
	err := tx.Where("workflow_id", "=", workflow.Id).Find(&results)
	if err != nil {
		panic(err)
	}

	// Should return pointers to the result objects so that
	// they can be mutated
	pointerResults := make([]*WorkflowBatch, len(results))
	for i := range results {
		pointerResults[i] = &results[i]
	}

	return pointerResults
}

func (workflowBatch *WorkflowBatch) getRdds(tx *hood.Hood) []*Rdd {
	var results []Rdd
	err := tx.Where("workflow_batch_id", "=", workflowBatch.Id).Find(&results)
	if err != nil {
		panic(err)
	}

	// Should return pointers to the result objects so that
	// they can be mutated
	pointerResults := make([]*Rdd, len(results))
	for i := range results {
		pointerResults[i] = &results[i]
	}

	return pointerResults
}
