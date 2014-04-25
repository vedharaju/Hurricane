package udf

import (
	"log"
	"os/exec"
	"strings"
	"worker"
)

// Execute a UDF command that accepts zero or more input lists of tuples, and
// returns one output list of tuples. This function blocks until the UDF is
// done executing.
func runUDF(command string, inputTuples ...[]worker.Tuple) []worker.Tuple {
	// spawn the external process
	splits := strings.Split(command, " ")
	cmd := exec.Command(splits[0], splits[1:]...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Panic(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Panic(err)
	}
	cmd.Start()

	// write tuples to standard input on a background goroutine
	go func() {
		for index, tupleList := range inputTuples {
			for _, tuple := range tupleList {
				stdin.Write(tuple.SerializeTuple(index))
				stdin.Write([]byte{'\n'})
			}
		}
		stdin.Close()
	}()

	// read from standard output to get the output tuples
	outputTuples := make([]worker.Tuple, 0)
	worker.ReadTupleStream(stdout, func(tuple worker.Tuple, index int) {
		outputTuples = append(outputTuples, tuple)
	})

	return outputTuples
}
