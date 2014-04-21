package master

import (
	"github.com/eaigner/hood"
  "time"
)

type Rdd struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// Reference to the job that created this RDD
	JobId int64

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}

type Segment struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// Reference to the RDD that this segment belongs to
	RddId int64

	// Reference to the task that created this segment
	TaskId int64

	// Reference to the worker where this segment resides
	WorkerID int64

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}

type Task struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// Reference to the job this task belongs to
	JobId int64

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}

type Job struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// Reference to the workflow that this job belongs to
	WorkflowId int64

	// Reference to the protojob that this job belongs to
	ProtojobId int64

  // Reference to the RDD that the input is stored on
  InputRddId int64

	// Reference to the RDD that the output is stored on
	OutputRddId int64

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

type Command struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// Name of the command, UDF or built-in
	Name string

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}

type Protojob struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}

type WorkflowBatch struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// Reference to the protojob this batch belongs to
	ProtoJobId int64

	// Start time of the batch
	StartTime time.Time

	// Duration of the batch (must be cast to time.Duration)
	Duration int64

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}

type WorkerNode struct {
	// Auto-incrementing int field 'id'
	Id hood.Id

	// URL of the worker node
	Url string

	// These fields are auto updated on save
	Created hood.Created
	Updated hood.Updated
}
