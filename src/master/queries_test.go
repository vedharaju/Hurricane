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

func cleanup(hd *hood.Hood) {
	ResetDb(hd)
}

func TestBasic(t *testing.T) {
	hd := GetTestDbConnection()
	setup(hd)
	defer cleanup(hd)

	fmt.Printf("Test: Basic RDD has segments ...\n")

	workflow := MockWorkflow(hd)
	protojob := MockProtojob(hd, workflow)
	workflowBatch := MockWorkflowBatch(hd, workflow)
	rdd := MockRdd(hd, workflowBatch, protojob)
	worker := MockWorker(hd)
	MockSegment(hd, rdd, worker)
	MockSegment(hd, rdd, worker)

	results := rdd.getSegments(hd)

	if len(results) != 2 {
		t.Fatalf("incorrect number of segments; wanted=%d got=%d", len(results), 2)
	}

	fmt.Printf("  ... Passed\n")

}
