package main

import "master"
import "workflow"
import "fmt"
import "os"

func printUsage() {
	fmt.Println("Usage\n  go run load_workflow.go filename\n")
}

func main() {
	if len(os.Args) != 2 {
		printUsage()
		return
	}

	hd := master.GetDbConnection()

	reader, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}

	w, err := workflow.ReadWorkflow(hd, reader)
	if err != nil {
		panic(err)
	}

	fmt.Println("Loaded workflow")
	fmt.Println("--------------------------Begin--------------------------")
	fmt.Println("Workflow ID:", w.Id)
	fmt.Println(workflow.WorkflowToString(hd, w))
	fmt.Println("---------------------------End---------------------------")
}
