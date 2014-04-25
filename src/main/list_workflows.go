package main

import "master"
import "workflow"
import "fmt"

func main() {
	hd := master.GetDbConnection()

	workflows := master.GetWorkflows(hd)

	for _, w := range workflows {
		fmt.Println("--------------------------Begin--------------------------")
		fmt.Println("Workflow ID:", w.Id)
		fmt.Println(workflow.WorkflowToString(hd, w))
		fmt.Println("---------------------------End---------------------------")
	}
}
