package workflow

import (
	"fmt"
	"io"
	"log"
	"master"
	"os"
	"testing"
)

func getReader(name string) io.Reader {
	reader, err := os.Open("test_files/" + name)
	if err != nil {
		log.Fatal(err)
	}
	return reader
}

func TestParsing(t *testing.T) {
	hd := master.GetTestDbConnection()
	master.ResetDb(hd)
	master.CreateTables(hd)
	client.Debug("Test Basic...")
	reader := getReader("test")
	if _, err := ReadWorkflow(hd, reader); err != nil {
		log.Fatal(err)
	}
	client.Debug("...passed")
	client.Debug("Test Repeated Job...")
	reader = getReader("test_repeated_job")
	if _, err := ReadWorkflow(hd, reader); err == nil {
		t.Fatalf("should have failed on repeated job")
	}
	client.Debug("...passed")
	client.Debug("Test Undefined Job...")
	reader = getReader("test_undefined_job")
	if _, err := ReadWorkflow(hd, reader); err == nil {
		t.Fatalf("should have failed on undefined job")
	}
	client.Debug("...passed")
}

func TestPrinting(t *testing.T) {
	hd := master.GetTestDbConnection()
	master.ResetDb(hd)
	master.CreateTables(hd)

	client.Debug("Test Printing...")
	reader := getReader("test")
	workflow, _ := ReadWorkflow(hd, reader)
	str := WorkflowToString(hd, workflow)
	client.Debug(str)
}
