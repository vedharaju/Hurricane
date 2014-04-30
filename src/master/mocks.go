package master

import (
	"github.com/eaigner/hood"
)

func saveOrPanic(hd *hood.Hood, x interface{}) {
	_, err := hd.Save(x)
	if err != nil {
		panic(err)
	}
}

func MockProtojob(hd *hood.Hood, workflow *Workflow) *Protojob {
	job := Protojob{
		Command:    "command",
		WorkflowId: int64(workflow.Id),
	}
	saveOrPanic(hd, &job)
	return &job
}

func MockWorkflow(hd *hood.Hood) *Workflow {
	workflow := Workflow{
		Duration: 100,
	}
	saveOrPanic(hd, &workflow)
	return &workflow
}

func MockWorkflowBatch(hd *hood.Hood, workflow *Workflow) *WorkflowBatch {
	wb := WorkflowBatch{
		WorkflowId: int64(workflow.Id),
		StartTime:  100,
	}
	saveOrPanic(hd, &wb)
	return &wb
}

func MockRdd(hd *hood.Hood, workflowBatch *WorkflowBatch, protojob *Protojob) *Rdd {
	rdd := Rdd{
		WorkflowBatchId: int64(workflowBatch.Id),
		ProtojobId:      int64(protojob.Id),
	}
	saveOrPanic(hd, &rdd)
	return &rdd
}

func MockWorker(hd *hood.Hood) *Worker {
	worker := Worker{Url: "url"}
	saveOrPanic(hd, &worker)
	return &worker
}

func MockSegment(hd *hood.Hood, rdd *Rdd, worker *Worker) *Segment {
	s := Segment{
		RddId:    int64(rdd.Id),
		WorkerId: int64(worker.Id),
	}
	saveOrPanic(hd, &s)
	return &s
}

func MockRddEdge(hd *hood.Hood, src *Rdd, dest *Rdd) *RddEdge {
	x := RddEdge{
		SourceRddId: int64(src.Id),
		DestRddId:   int64(dest.Id),
	}
	saveOrPanic(hd, &x)
	return &x
}

func MockWorkflowEdge(hd *hood.Hood, src *Protojob, dest *Protojob) *WorkflowEdge {
	x := WorkflowEdge{
		SourceJobId: int64(src.Id),
		DestJobId:   int64(dest.Id),
	}
	saveOrPanic(hd, &x)
	return &x
}

// Create a Workflow with one WorkflowBatch. The Protojobs are
// organized as follows (arrows point from top to bottom).
//
//        1
//       / \
//      2   3
//       \ /
//        4
//
// There are 2 workers. Each RDD has segments on both workers.
func MockDiamondWorkflow(hd *hood.Hood) *Workflow {
	workflow := MockWorkflow(hd)
	j1 := MockProtojob(hd, workflow)
	j2 := MockProtojob(hd, workflow)
	j3 := MockProtojob(hd, workflow)
	j4 := MockProtojob(hd, workflow)
	MockWorkflowEdge(hd, j1, j2)
	MockWorkflowEdge(hd, j1, j3)
	MockWorkflowEdge(hd, j2, j4)
	MockWorkflowEdge(hd, j3, j4)

	workflowBatch := MockWorkflowBatch(hd, workflow)
	rdd1 := MockRdd(hd, workflowBatch, j1)
	rdd2 := MockRdd(hd, workflowBatch, j2)
	rdd3 := MockRdd(hd, workflowBatch, j3)
	rdd4 := MockRdd(hd, workflowBatch, j4)
	MockRddEdge(hd, rdd1, rdd2)
	MockRddEdge(hd, rdd1, rdd3)
	MockRddEdge(hd, rdd2, rdd4)
	MockRddEdge(hd, rdd3, rdd4)

	w1 := MockWorker(hd)
	w2 := MockWorker(hd)

	// Place a segment for every rdd on every worker
	rdds := []*Rdd{rdd1, rdd2, rdd3, rdd4}
	workers := []*Worker{w1, w2}
	for _, rdd := range rdds {
		for _, worker := range workers {
			MockSegment(hd, rdd, worker)
		}
	}

	return workflow
}
