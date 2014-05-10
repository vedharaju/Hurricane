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

func parseCommand(command string) (string, map[string]string, error) {
	r_doublesemi, _ := regexp.Compile("\\s*;;\\s*")
	r_amp, _ := regexp.Compile("\\s*&\\s*")
	r_eq, _ := regexp.Compile("\\s*=\\s*")

	args := r_doublesemi.Split(strings.TrimSpace(command), 2)
	cmd := args[0]

	cmd_args := make(map[string]string)
	if len(args) == 2 {
		args_separated := r_amp.Split(strings.TrimSpace(args[1]), -1)
		for _, element := range args_separated {
			key_arg := r_eq.Split(strings.TrimSpace(element), 2)
			if len(key_arg) == 2 {
				cmd_args[strings.TrimSpace(key_arg[0])] = strings.TrimSpace(key_arg[1])
			} else {
				// TODO: figure out if error needs to be thrown
			}
		}
	}
	return cmd, cmd_args, nil
}

func makeProtoJob(hd *hood.Hood, workflow *master.Workflow, command string) *master.Protojob {
	//TODO: error handling
	cmd, args, _ := parseCommand(command) //TODO: handle error from parseCommand
	job := master.Protojob{
		Command:    cmd,
		WorkflowId: int64(workflow.Id),
	}

	for k, v := range args {
		if k == "r" {
			if v == "true" {
				job.IsReduce = true
			} else if v == "false" {
				job.IsReduce = false
			} else {
				panic("v should be true or false")
			}
		} else if k == "p" {
			job.PartitionIndex = v
		} else if k == "w" {
			num, err := strconv.ParseInt(v, 10, 0)
			if err == nil {
				job.NumSegments = int(num)
			} else {
				panic("w should be an int")
			}
		} else if k == "b" {
			num, err := strconv.ParseInt(v, 10, 0)
			if err == nil {
				job.NumBuckets = int(num)
			} else {
				panic("b should be an int")
			}
		} else if k == "c" {
			num, err := strconv.ParseInt(v, 10, 0)
			if err == nil {
				job.Copies = int(num)
			} else {
				panic("c should be an int")
			}
		}
	}

	saveOrPanic(hd, &job)
	return &job
}

func makeWorkflow(hd *hood.Hood) *master.Workflow {
	workflow := master.Workflow{}
	saveOrPanic(hd, &workflow)
	return &workflow
}

func makeWorkflowEdge(hd *hood.Hood, src int64, dest int64, delay int, index int) *master.WorkflowEdge {
	edge := master.WorkflowEdge{
		SourceJobId: src,
		DestJobId:   dest,
		Delay:       delay,
		Index:       index,
	}
	saveOrPanic(hd, &edge)
	return &edge
}

func ReadWorkflow(hd *hood.Hood, inputReader io.Reader) (*master.Workflow, error) {
	mode := NONE
	scanner := bufio.NewScanner(inputReader)

	r_job, _ := regexp.Compile("JOBS.*")
	r_workflow, _ := regexp.Compile("WORKFLOW.*")
	r_jobChar, _ := regexp.Compile("\\s*:\\s*")
	r_workflowChar, _ := regexp.Compile("\\s*->\\s*")
	r_comma, _ := regexp.Compile("\\s*,\\s*")
	r_whitespace, _ := regexp.Compile("\\s+")

	workflow := makeWorkflow(hd)
	jobIds := make(map[string]int64)

	for scanner.Scan() {
		line := scanner.Text()

		if mode == NONE {
			if r_job.MatchString(line) {
				mode = JOB
				saveOrPanic(hd, workflow)
			} else {
				splits := strings.Split(line, "=")
				if len(splits) == 2 {
					key := strings.TrimSpace(splits[0])
					value := strings.TrimSpace(splits[1])
					if key == "d" {
						duration, err := strconv.Atoi(value)
						if (err != nil) || (duration < 0) {
							return nil, errors.New("header: invalid duration")
						}
						workflow.Duration = int64(duration)
					}
				}
			}
		} else if mode == JOB {
			if r_workflow.MatchString(line) {
				mode = WORKFLOW
			} else {
				split := r_jobChar.Split(strings.TrimSpace(line), 2)
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
			split := r_workflowChar.Split(strings.TrimSpace(line), 2)

			if len(split) >= 2 {
				toId, ok := jobIds[split[1]]
				if !ok {
					return nil, errors.New("jobs: Undefined job " + split[1])
				}

				from := r_comma.Split(strings.TrimSpace(split[0]), -1)
				for index, fromJob := range from {
					fromInfo := r_whitespace.Split(strings.TrimSpace(fromJob), 2)
					fromId, ok := jobIds[strings.TrimSpace(fromInfo[0])]

					var fromDelay int64
					//var err error
					if len(fromInfo) == 2 {
						fromDelay, _ = strconv.ParseInt(fromInfo[1], 10, 0) //TODO: handle error
					}

					if ok {
						makeWorkflowEdge(hd, fromId, toId, int(fromDelay), index)
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
