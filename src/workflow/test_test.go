package workflow

import (
	"fmt"
	"log"
	"master"
	"testing"
)

func TestParsing(t *testing.T) {
	hd := master.GetTestDbConnection()
	master.ResetDb(hd)
	master.CreateTables(hd)
	fmt.Println("Test Basic...")
	if _, err := readWorkflow(hd, "test_files/test"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("...passed")
	fmt.Println("Test Repeated Job...")
	if _, err := readWorkflow(hd, "test_files/test_repeated_job"); err == nil {
		t.Fatalf("should have failed on repeated job")
	}
	fmt.Println("...passed")
	fmt.Println("Test Undefined Job...")
	if _, err := readWorkflow(hd, "test_files/test_undefined_job"); err == nil {
		t.Fatalf("should have failed on undefined job")
	}
	fmt.Println("...passed")
}

func TestPrinting(t *testing.T) {
	hd := master.GetTestDbConnection()
	master.ResetDb(hd)
	master.CreateTables(hd)

	fmt.Println("Test Printing...")
	workflow, _ := readWorkflow(hd, "test_files/test")
	str := workflowToString(hd, workflow)
	fmt.Println(str)
}
