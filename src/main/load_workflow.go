package main

import "master"
import "workflow"
import "client"
import "os"

func printUsage() {
	client.Debug("Usage\n  go run load_workflow.go filename\n")
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

	client.Debug("Loaded workflow")
	client.Debug("--------------------------Begin--------------------------")
	client.Debug("Workflow ID:", w.Id)
	client.Debug(workflow.WorkflowToString(hd, w))
	client.Debug("---------------------------End---------------------------")
}
