package master

import (
	"fmt"
	"github.com/eaigner/hood"
	"testing"
)

func setup(hd *hood.Hood) {
	ResetDb(hd)
	CreateTables(hd)
}

func TestBasic(t *testing.T) {
	hd := GetTestDbConnection()
	setup(hd)

	fmt.Printf("Test: Basic RDD has segments ...\n")

	workflow := MockWorkflow(hd)
	protojob := MockProtojob(hd, workflow)
	workflowBatch := MockWorkflowBatch(hd, workflow)
	rdd := MockRdd(hd, workflowBatch, protojob)
	worker := MockWorker(hd)
	MockSegment(hd, rdd, worker)
	MockSegment(hd, rdd, worker)

	results := rdd.GetSegments(hd)

	if len(results) != 2 {
		t.Fatalf("incorrect number of segments; got=%d wanted=%d", len(results), 2)
	}

	fmt.Printf("  ... Passed\n")

}

func TestComplex(t *testing.T) {
	hd := GetTestDbConnection()
	setup(hd)

	fmt.Printf("Test: Diamond Workflow Queries ...\n")

	workflow := MockDiamondWorkflow(hd)

	// test protojobs
	pjs := workflow.GetProtojobs(hd)
	if len(pjs) != 4 {
		t.Fatalf("incorrect number of protojobs; got=%d wanted=%d", len(pjs), 4)
	}

	// test edges
	wes := workflow.GetWorkflowEdges(hd)
	if len(wes) != 4 {
		t.Fatalf("incorrect number of workflow edges; got=%d wanted=%d", len(wes), 4)
	}

	// test batches
	wbs := workflow.GetWorkflowBatches(hd)
	if len(wbs) != 1 {
		t.Fatalf("incorrect number of workflow batches; got=%d wanted=%d", len(wbs), 1)
	}

	wb := wbs[0]
	rdds := wb.GetRdds(hd)
	if len(rdds) != 4 {
		t.Fatalf("incorrect number of rdds; got=%d wanted=%d", len(wbs), 4)
	}

	rddes := wb.GetRddEdges(hd)
	if len(rddes) != 4 {
		t.Fatalf("incorrect number of rdd edges; got=%d wanted=%d", len(wbs), 4)
	}
}
