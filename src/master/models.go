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
  DestRddId int64

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
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
  DestJobId int64

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
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

type Worker struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// URL of the worker node
	Url string

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}
