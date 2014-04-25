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
	fmt.Println("Test Basic...")
	reader := getReader("test")
	if _, err := readWorkflow(hd, reader); err != nil {
		log.Fatal(err)
	}
	fmt.Println("...passed")
	fmt.Println("Test Repeated Job...")
	reader = getReader("test_repeated_job")
	if _, err := readWorkflow(hd, reader); err == nil {
		t.Fatalf("should have failed on repeated job")
	}
	fmt.Println("...passed")
	fmt.Println("Test Undefined Job...")
	reader = getReader("test_undefined_job")
	if _, err := readWorkflow(hd, reader); err == nil {
		t.Fatalf("should have failed on undefined job")
	}
	fmt.Println("...passed")
}

func TestPrinting(t *testing.T) {
	hd := master.GetTestDbConnection()
	master.ResetDb(hd)
	master.CreateTables(hd)

	fmt.Println("Test Printing...")
	reader := getReader("test")
	workflow, _ := readWorkflow(hd, reader)
	str := WorkflowToString(hd, workflow)
	fmt.Println(str)
}
