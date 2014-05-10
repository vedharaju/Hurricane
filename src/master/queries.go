package master

import (
	"github.com/eaigner/hood"
)

type IntStruct struct {
	Value int
}

func (rdd *Rdd) GetSegments(tx *hood.Hood) []*Segment {
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

func (workflow *Workflow) GetProtojobs(tx *hood.Hood) []*Protojob {
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
func (workflow *Workflow) GetWorkflowEdges(tx *hood.Hood) []*WorkflowEdge {
	var results []WorkflowEdge
	err := tx.FindSql(&results,
		`select workflow_edge.*
    from workflow_edge
    inner join protojob dest_job
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

// Get all Rdd edges whose dest Rdd is in the given workflow batch. Note that
// the source and dest Rdds may be produced in different batches.
func (workflowBatch *WorkflowBatch) GetRddEdges(tx *hood.Hood) []*RddEdge {
	var results []RddEdge
	err := tx.FindSql(&results,
		`select rdd_edge.*
    from rdd_edge
    inner join rdd dest_rdd
    on rdd_edge.dest_rdd_id = dest_rdd.id
    where dest_rdd.workflow_batch_id = $1`, workflowBatch.Id)
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

// Same as GetRddEdges, except the edges cannot have any delay
func (workflowBatch *WorkflowBatch) GetNonDelayRddEdges(tx *hood.Hood) []*RddEdge {
	var results []RddEdge
	err := tx.FindSql(&results,
		`select rdd_edge.*
    from rdd_edge
    inner join rdd dest_rdd
    on rdd_edge.dest_rdd_id = dest_rdd.id
    inner join workflow_edge
    on workflow_edge.id = rdd_edge.workflow_edge_id
    where dest_rdd.workflow_batch_id = $1
    and workflow_edge.delay=0`, workflowBatch.Id)
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

func (workflow *Workflow) GetWorkflowBatches(tx *hood.Hood) []*WorkflowBatch {
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

func (workflowBatch *WorkflowBatch) GetRdds(tx *hood.Hood) []*Rdd {
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

func GetWorkflows(tx *hood.Hood) []*Workflow {
	var results []Workflow
	err := tx.Find(&results)
	if err != nil {
		panic(err)
	}

	// Should return pointers to the result objects so that
	// they can be mutated
	pointerResults := make([]*Workflow, len(results))
	for i := range results {
		pointerResults[i] = &results[i]
	}

	return pointerResults
}

func GetWorkers(tx *hood.Hood) []*Worker {
	var results []Worker
	err := tx.Find(&results)
	if err != nil {
		panic(err)
	}

	// Should return pointers to the result objects so that
	// they can be mutated
	pointerResults := make([]*Worker, len(results))
	for i := range results {
		pointerResults[i] = &results[i]
	}

	return pointerResults
}

func GetAliveWorkers(tx *hood.Hood) []*Worker {
	var results []Worker
	err := tx.Where("status", "=", WORKER_ALIVE).Find(&results)
	if err != nil {
		panic(err)
	}

	// Should return pointers to the result objects so that
	// they can be mutated
	pointerResults := make([]*Worker, len(results))
	for i := range results {
		pointerResults[i] = &results[i]
	}

	return pointerResults
}

func GetWorkersAtAddress(tx *hood.Hood, address string) []*Worker {
	var results []Worker
	err := tx.Where("url", "=", address).Find(&results)
	if err != nil {
		panic(err)
	}

	// Should return pointers to the result objects so that
	// they can be mutated
	pointerResults := make([]*Worker, len(results))
	for i := range results {
		pointerResults[i] = &results[i]
	}

	return pointerResults
}

func GetWorker(tx *hood.Hood, id int64) *Worker {
	var results []Worker
	err := tx.Where("id", "=", id).Find(&results)
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		return nil
	} else {
		return &results[0]
	}
}

func GetRddByStartTime(tx *hood.Hood, protojobId int64, startTime int64) *Rdd {
	var results []Rdd
	err := tx.FindSql(&results,
		`select rdd.*
  from rdd
  inner join workflow_batch
  on rdd.workflow_batch_id = workflow_batch.id
  where rdd.protojob_id = $1
  and workflow_batch.start_time = $2`, protojobId, startTime)
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		return nil
	} else {
		return &results[0]
	}
}

func (workflow *Workflow) GetLastWorkflowBatch(tx *hood.Hood) *WorkflowBatch {
	var results []WorkflowBatch
	err := tx.Where("workflow_id", "=", workflow.Id).OrderBy("start_time").Desc().Limit(1).Find(&results)
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		return nil
	} else {
		return &results[0]
	}
}

func (rdd *Rdd) GetProtojob(tx *hood.Hood) *Protojob {
	var results []Protojob
	err := tx.Where("id", "=", rdd.ProtojobId).Find(&results)
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		panic("Rdd is missing protojob")
	} else {
		return &results[0]
	}
}

func GetWorkflow(tx *hood.Hood, workflowId int64) *Workflow {
	var results []Workflow
	err := tx.Where("id", "=", workflowId).Find(&results)
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		panic("could not find workflow with given id")
	} else {
		return &results[0]
	}
}

func GetRdd(tx *hood.Hood, rddId int64) *Rdd {
	var results []Rdd
	err := tx.Where("id", "=", rddId).Find(&results)
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		panic("could not find rdd with given id")
	} else {
		return &results[0]
	}
}

func (rdd *Rdd) GetInputEdges(tx *hood.Hood) []*RddEdge {
	var results []RddEdge
	err := tx.Where("dest_rdd_id", "=", rdd.Id).Find(&results)
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

func GetSegment(tx *hood.Hood, segmentId int64) *Segment {
	var results []Segment
	err := tx.Where("id", "=", segmentId).Find(&results)
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		panic("could not find segment with given id")
	} else {
		return &results[0]
	}
}

func (segment *Segment) GetRdd(tx *hood.Hood) *Rdd {
	var results []Rdd
	err := tx.Where("id", "=", segment.RddId).Find(&results)
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		panic("could not find rdd with given id")
	} else {
		return &results[0]
	}
}

func (segment *Segment) GetSegmentCopies(tx *hood.Hood) []*SegmentCopy {
	var results []SegmentCopy
	err := tx.Where("segment_id", "=", segment.Id).Find(&results)
	if err != nil {
		panic(err)
	}

	// Should return pointers to the result objects so that
	// they can be mutated
	pointerResults := make([]*SegmentCopy, len(results))
	for i := range results {
		pointerResults[i] = &results[i]
	}

	return pointerResults
}

func (rdd *Rdd) GetSegmentCopies(tx *hood.Hood) []*SegmentCopy {
	var results []SegmentCopy
	err := tx.FindSql(&results,
		`select segment_copy.*
    from segment_copy
    inner join segment
    on segment_copy.segment_id = segment.id
    where segment.rdd_id = $1`, rdd.Id)
	if err != nil {
		panic(err)
	}

	// Should return pointers to the result objects so that
	// they can be mutated
	pointerResults := make([]*SegmentCopy, len(results))
	for i := range results {
		pointerResults[i] = &results[i]
	}

	return pointerResults
}

func GetSegmentCopy(tx *hood.Hood, segmentCopyId int64) *SegmentCopy {
	var results []SegmentCopy
	err := tx.Where("id", "=", segmentCopyId).Find(&results)
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		panic("could not find segment copy with given id")
	} else {
		return &results[0]
	}
}

func (segmentCopy *SegmentCopy) GetSegment(tx *hood.Hood) *Segment {
	var results []Segment
	err := tx.Where("id", "=", segmentCopy.SegmentId).Find(&results)
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		panic("could not find rdd with given id")
	} else {
		return &results[0]
	}
}

func (protojob *Protojob) GetInputEdges(tx *hood.Hood) []*WorkflowEdge {
	var results []WorkflowEdge
	err := tx.Where("dest_job_id", "=", protojob.Id).Find(&results)
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

func (protojob *Protojob) GetSourceProtojobs(tx *hood.Hood) []*Protojob {
	var results []Protojob
	err := tx.FindSql(&results,
		`select source_job.*
    from workflow_edge
    inner join protojob source_job
    on workflow_edge.source_job_id = source_job.id
    where workflow_edge.dest_job_id = $1`, protojob.Id)
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

func (rdd *Rdd) GetSourceRdds(tx *hood.Hood) []*Rdd {
	var results []Rdd
	err := tx.FindSql(&results,
		`select source_rdd.*
    from rdd_edge
    inner join rdd source_rdd
    on rdd_edge.source_rdd_id = source_rdd.id
    where rdd_edge.dest_rdd_id = $1`, rdd.Id)
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

func GetPendingRdds(tx *hood.Hood) []*Rdd {
	var results []Rdd
	err := tx.FindSql(&results,
		`select rdd.*
    from rdd
    where rdd.state = $1`, RDD_PENDING)
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

func (rdd *Rdd) GetDestRdds(tx *hood.Hood) []*Rdd {
	var results []Rdd
	err := tx.FindSql(&results,
		`select dest_rdd.*
    from rdd_edge
    inner join rdd dest_rdd
    on rdd_edge.dest_rdd_id = dest_rdd.id
    where rdd_edge.source_rdd_id = $1`, rdd.Id)
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

func (rdd *Rdd) GetNumSegmentsComplete(tx *hood.Hood, include *Segment) int {
	var results []IntStruct
	err := tx.FindSql(&results,
		`select count(*) as value
    from segment
    where (rdd_id = $1 and status=$3)
    or id = $2`, rdd.Id, include.Id, SEGMENT_COMPLETE)
	if err != nil {
		panic(err)
	}

	return results[0].Value
}

func (segment *Segment) GetWorker(tx *hood.Hood) *Worker {
	var results []Worker
	err := tx.Where("id", "=", segment.WorkerId).Find(&results)
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		panic("could not find worker for segment")
	} else {
		return &results[0]
	}
}

func GetNumAliveWorkers(tx *hood.Hood) int {
	var results []IntStruct
	err := tx.FindSql(&results,
		`select count(*) as value
    from worker
    where status=$1`, WORKER_ALIVE)
	if err != nil {
		panic(err)
	}

	return results[0].Value
}

func GetRandomAliveWorker(tx *hood.Hood) *Worker {
	var results []Worker
	err := tx.FindSql(&results,
		`select *
    from worker
    where status=$1
    order by random()
    limit 1`, WORKER_ALIVE)
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		return nil
	} else {
		return &results[0]
	}
}

func (rdd *Rdd) GetWorkflowBatch(tx *hood.Hood) *WorkflowBatch {
	var results []WorkflowBatch
	err := tx.Where("id", "=", rdd.WorkflowBatchId).Find(&results)
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		panic("workflow batch for rdd not found")
	} else {
		return &results[0]
	}
}

func (workflowBatch *WorkflowBatch) GetWorkflow(tx *hood.Hood) *Workflow {
	var results []Workflow
	err := tx.Where("id", "=", workflowBatch.WorkflowId).Find(&results)
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		panic("could not find workflow for the given workflow batch")
	} else {
		return &results[0]
	}
}
