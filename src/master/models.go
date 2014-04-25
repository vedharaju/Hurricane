package master

import (
	"github.com/eaigner/hood"
)

// Dependency between two RDDs
type RddEdge struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// References to the Rdd table
	SourceRddId int64
	DestRddId   int64

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}

func (table *RddEdge) Indexes(indexes *hood.Indexes) {
	indexes.Add("rdd_edge__source_rdd_id", "source_rdd_id")
	indexes.Add("rdd_edge__dest_rdd_id", "dest_rdd_id")
}

// RDD and "Job" are stored in the same data structure because every
// RDD was produced by exactly one job
type Rdd struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// Reference to the workflow batch that created this RDD
	WorkflowBatchId int64

	// Reference to the protojob that is executed to construct the RDD
	ProtojobId int64

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}

func (table *Rdd) Indexes(indexes *hood.Indexes) {
	indexes.Add("rdd__workflow_batch_id", "workflow_batch_id")
	indexes.Add("rdd__protojob_id", "protojob_id")
}

// Segments and "Tasks" are store in the same data structure
// because each segment is produced by exactly one task
type Segment struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// Reference to the RDD that this segment belongs to
	RddId int64

	// Reference to the worker where this task is executing
	WorkerId int64

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}

func (table *Segment) Indexes(indexes *hood.Indexes) {
	indexes.Add("segment__rdd_id", "rdd_id")
	indexes.Add("segment__worker_id", "worker_id")
}

type Workflow struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}

type WorkflowEdge struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// References into Protojob table
	SourceJobId int64
	DestJobId   int64

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}

func (table *WorkflowEdge) Indexes(indexes *hood.Indexes) {
	indexes.Add("workflow_edge__source_job_id", "source_job_id")
	indexes.Add("workflow_edge__dest_job_id", "dest_job_id")
}

type Protojob struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// Reference to the workflow
	WorkflowId int64

	// Name of the command, UDF or built-in
	Command string

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}

func (table *Protojob) Indexes(indexes *hood.Indexes) {
	indexes.Add("protojob__workflow_id", "workflow_id")
}

type WorkflowBatch struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// Reference to the workflow this batch belongs to
	WorkflowId int64

	// Start time of the batch (unix timestamp UTC)
	StartTime int

	// Duration of the batch (milliseconds)
	Duration int

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}

func (table *WorkflowBatch) Indexes(indexes *hood.Indexes) {
	indexes.Add("workflow_batch__workflow_id", "workflow_id")
}

type Worker struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// URL or IP address of the worker node
	Url string
	// TCP port of the worker's RPC handler
	Port int

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}
