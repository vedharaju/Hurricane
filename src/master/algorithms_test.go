package master

import (
	"testing"
)

func TestMakeBatch(t *testing.T) {
	hd := GetTestDbConnection()
	ResetDb(hd)
	CreateTables(hd)

	workflow := MockDiamondWorkflow(hd)

	lastBatch := workflow.GetLastWorkflowBatch(hd)
	if lastBatch == nil {
		t.Fatalf("last batch not found")
	}

	wb := workflow.MakeBatch(hd, lastBatch.StartTime+workflow.Duration)

	// there should be two batches, because the diamond workflow started with one
	wbs := workflow.GetWorkflowBatches(hd)
	if len(wbs) != 2 {
		t.Fatalf("incorrect number of workflow batches; got=%d wanted=%d", len(wbs), 2)
	}

	// check that the new batch has all of the rdds and edges
	rdds := wb.GetRdds(hd)
	if len(rdds) != 4 {
		t.Fatalf("incorrect number of rdds; got=%d wanted=%d", len(wbs), 4)
	}

	rddes := wb.GetRddEdges(hd)
	if len(rddes) != 4 {
		t.Fatalf("incorrect number of rdd edges; got=%d wanted=%d", len(wbs), 4)
	}

	if wb.StartTime != 200 {
		t.Fatalf("wrong start time; got=%d wanted=%d", wb.StartTime, 200)
	}
}

func TestFindSourceRdds(t *testing.T) {
	hd := GetTestDbConnection()
	ResetDb(hd)
	CreateTables(hd)

	workflow := MockDiamondWorkflow(hd)

	lastBatch := workflow.GetLastWorkflowBatch(hd)
	if lastBatch == nil {
		t.Fatalf("last batch not found")
	}

	rdds := lastBatch.FindSourceRdds(hd)

	if len(rdds) != 1 {
		t.Fatalf("wrong number of source rdds; got=%d wanted=%d", len(rdds), 1)
	}
}

func TestCreateSegments(t *testing.T) {
	hd := GetTestDbConnection()
	ResetDb(hd)
	CreateTables(hd)

	w := MockDiamondWorkflow(hd)
	j := MockProtojob(hd, w)
	wb := MockWorkflowBatch(hd, w)
	rdd := MockRdd(hd, wb, j)

	segments, _ := rdd.CreateSegments(hd)

	if len(segments) != 2 {
		t.Fatalf("wrong number of segments; got=%d wanted=%d", len(segments), 2)
	}
}
