package master

import (
	"github.com/eaigner/hood"
)

const (
	SEGMENT_COMPLETE = 1
	SEGMENT_PENDING  = 0

	RDD_PENDING  = 0
	RDD_COMPLETE = 1
)

// Dependency between two RDDs
type RddEdge struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// References to the Rdd table
	SourceRddId int64
	DestRddId   int64

	// Reference to the WorkflowEdge used to create this
	WorkflowEdgeId int64

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}

func (table *RddEdge) Indexes(indexes *hood.Indexes) {
	indexes.Add("rdd_edge__source_rdd_id", "source_rdd_id")
	indexes.Add("rdd_edge__dest_rdd_id", "dest_rdd_id")
	indexes.Add("rdd_edge__workflow_edge_id", "workflow_edge_id")
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

	// Current state of Rdd, default pending
	State int64

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

	// Index of the segment within the RDD (between 0 and RDD.Protojob.NumSegments)
	Index int

	// Status of Segment (SEGMENT_COMPLETE vs SEGMENT_PENDING)
	Status int

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

	// Duration of the batch (milliseconds)
	Duration int64

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

	// Delay of the input RDD. 0 means no delay
	Delay int

	// Index of the input for an RDD. 0 is the first input
	Index int

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

	// Partition vector index, in the format (n1,n2,n3)
	PartitionIndex string

	// Number of segments in the output RDD - same as the number of workers to be used
	NumSegments int

	// Number of partition buckets
	NumBuckets int

	// True if the input segments should be grouped by partition
	IsReduce bool

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
	StartTime int64

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}

func (table *WorkflowBatch) Indexes(indexes *hood.Indexes) {
	indexes.Add("workflow_batch__workflow_id", "workflow_id")
	indexes.Add("workflow_batch__start_time", "start_time")
}

type Worker struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// URL and port of the worker node
	Url string

	// false if the worker is alive
	Dead bool

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}
