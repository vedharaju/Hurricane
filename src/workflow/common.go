package workflow

import (
  "os"
  "bufio"
  "fmt"
  "log"
  "regexp"
  "strings"
  "master"
  "github.com/eaigner/hood"
)

const (
  JOB = "JOBS"
  WORKFLOW = "WORKFLOW"
  NONE = "None"
)

func saveOrPanic(hd *hood.Hood, x interface{}) {
        _, err := hd.Save(x)
        if err != nil {
                panic(err)
        }
}


func makeProtoJob(hd *hood.Hood, workflow *master.Workflow, command string) *master.ProtoJob{
  job := Protojob{
    Command:    command,
    WorkflowId: int64(workflow.Id),
  }
  saveOrPanic(hd, &job)
  return &job
}

func makeWorkflow(hd *hood.Hood) *master.Workflow{
  workflow := Workflow{}
  saveOrPanic(hd, &workflow)
  return &workflow
}

func makeWorkflowEdge(hd *hood.Hood, src *master.Protojob, dest *masterProtojob) *master.WorkflowEdge{
  x := WorkflowEdge{
    SourceJobId: int64(src.Id),
    DestJobId:   int64(dest.Id),
  }
  saveOrPanic(hd, &x)
  return &x
}

func parse(r *regexp.Regexp, line string, n int) []string {
    return r.Split(line, n)
}

func readWorkflow() {
  mode := NONE 
  file, _ := os.Open("test")
  scanner := bufio.NewScanner(file)
  //scanner := bufio.NewScanner(os.Stdin)

  r_job, _ := regexp.Compile("JOBS.*")  
  r_workflow, _ := regexp.Compile("WORKFLOW.*")
  r_jobChar, _ := regexp.Compile("\\s*:\\s*")
  r_workflowChar, _ := regexp.Compile("\\s*->\\s*")
  r_comma, _ := regexp.Compile("\\s*,\\s*")

  for scanner.Scan() {
    line := scanner.Text()
 
   if mode==NONE && r_job.MatchString(line) {
      mode = JOB 
    } else if mode==JOB {
      if r_workflow.MatchString(line) {
        mode = WORKFLOW
      } else {
        split:= parse(r_jobChar, strings.TrimSpace(line), 2)
        if len(split) >= 2 {
          //TODO: make this job
        } else {
          //TODO: check if error or just whitespace
        }
      }
    } else if mode==WORKFLOW {
      split:= parse(r_workflowChar, strings.TrimSpace(line), 2)
      if len(split) >= 2 {
        from := parse(r_comma, strings.TrimSpace(split[0]), -1)
        //to := parse(r_comma, strings.TrimSpace(split[1]), -1)
      }  else {
          //TODO: check if error or just whitespace
      }
    } 
    
  }

  if err := scanner.Err(); err != nil {
    log.Fatal(err)
  }
}
