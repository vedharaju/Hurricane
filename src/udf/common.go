package main

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
        "io"
)

// Functions and data structures for spawning a subprocess as a UDF

func runUDF(command string, inputTuples string, args ...string) {
  cmd := exec.Command(command, args...)
  stdin, err := cmd.StdinPipe()
  if err != nil {
    log.Panic(err)
  }
  io.Copy(stdin, bytes.NewBufferString("Vedha"))
  var out bytes.Buffer
  cmd.Stdout = &out
  err = cmd.Run()
  if err != nil {
    log.Fatal(err)
  }
  fmt.Printf(out.String())
}

func main() {
  args := []string{"test.py"}
  runUDF("python", "", args...)
}
