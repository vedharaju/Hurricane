package workflow

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/eaigner/hood"
	"io"
	"log"
	"master"
	"regexp"
	"strconv"
	"strings"
)

const (
	JOB      = "JOBS"
	WORKFLOW = "WORKFLOW"
	NONE     = "None"
)

func saveOrPanic(hd *hood.Hood, x interface{}) {
	_, err := hd.Save(x)
	if err != nil {
		panic(err)
	}
}

func makeProtoJob(hd *hood.Hood, workflow *master.Workflow, command string) *master.Protojob {
	job := master.Protojob{
		Command:    command,
		WorkflowId: int64(workflow.Id),
	}
	saveOrPanic(hd, &job)
	return &job
}

func makeWorkflow(hd *hood.Hood) *master.Workflow {
	workflow := master.Workflow{}
	saveOrPanic(hd, &workflow)
	return &workflow
}

func makeWorkflowEdge(hd *hood.Hood, src int64, dest int64) *master.WorkflowEdge {
	x := master.WorkflowEdge{
		SourceJobId: src,
		DestJobId:   dest,
	}
	saveOrPanic(hd, &x)
	return &x
}

func parse(r *regexp.Regexp, line string, n int) []string {
	return r.Split(line, n)
}

func readWorkflow(hd *hood.Hood, inputReader io.Reader) (*master.Workflow, error) {
	mode := NONE
	scanner := bufio.NewScanner(inputReader)

	r_job, _ := regexp.Compile("JOBS.*")
	r_workflow, _ := regexp.Compile("WORKFLOW.*")
	r_jobChar, _ := regexp.Compile("\\s*:\\s*")
	r_workflowChar, _ := regexp.Compile("\\s*->\\s*")
	r_comma, _ := regexp.Compile("\\s*,\\s*")

	workflow := makeWorkflow(hd)
	jobIds := make(map[string]int64)

	for scanner.Scan() {
		line := scanner.Text()

		if mode == NONE && r_job.MatchString(line) {
			mode = JOB
		} else if mode == JOB {
			if r_workflow.MatchString(line) {
				mode = WORKFLOW
			} else {
				split := parse(r_jobChar, strings.TrimSpace(line), 2)
				if len(split) >= 2 {
					if _, ok := jobIds[split[0]]; !ok {
						job := makeProtoJob(hd, workflow, split[1])
						jobIds[split[0]] = int64(job.Id)
					} else {
						return nil, errors.New("jobs: Multiple jobs declared with the same name")
					}

				} else {
					if len(split) != 1 || split[0] != "" {
						return nil, errors.New("workflow: Invalid syntax in job declaration ")
					}
				}
			}
		} else if mode == WORKFLOW {
			split := parse(r_workflowChar, strings.TrimSpace(line), 2)

			if len(split) >= 2 {
				toId, ok := jobIds[split[1]]
				if !ok {
					return nil, errors.New("jobs: Undefined job " + split[1])
				}

				from := parse(r_comma, strings.TrimSpace(split[0]), -1)
				for _, fromJob := range from {
					fromId, ok := jobIds[fromJob]
					if ok {
						makeWorkflowEdge(hd, fromId, toId)
					} else {
						return nil, errors.New("jobs: Undefined job " + fromJob)
					}
				}
			} else {
				if len(split) != 1 || split[0] != "" {
					return nil, errors.New("workflow: Invalid syntax in job declaration ")
				}
			}
		}

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return workflow, nil
}

func WorkflowToString(hd *hood.Hood, w *master.Workflow) string {
	jobs := w.GetProtojobs(hd)
	edges := w.GetWorkflowEdges(hd)

	output := "JOBS\n\n"
	for _, job := range jobs {
		output += fmt.Sprintf("%v: %v\n", job.Id, job.Command)
	}
	output += "\nWORKFLOW\n\n"
	inEdges := make(map[int64][]int64)
	for _, edge := range edges {
		inEdges[edge.DestJobId] = append(inEdges[edge.DestJobId], edge.SourceJobId)
	}
	for dest, srcs := range inEdges {
		srcStrings := make([]string, len(srcs))
		for i, src := range srcs {
			srcStrings[i] = strconv.FormatInt(src, 10)
		}
		joined := strings.Join(srcStrings, ",")
		output += fmt.Sprintf("%v: %v\n", joined, strconv.FormatInt(dest, 10))
	}

	return output
}
