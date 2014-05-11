package main

import "master"
import "workflow"
import "client"

func main() {
	hd := master.GetDbConnection()

	workflows := master.GetWorkflows(hd)

	for _, w := range workflows {
		client.Debug("--------------------------Begin--------------------------")
		client.Debug("Workflow ID:", w.Id)
		client.Debug(workflow.WorkflowToString(hd, w))
		client.Debug("---------------------------End---------------------------")
	}
}
