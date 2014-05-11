package main

import "os"
import "syscall"
import "worker"
import "strings"

func main() {

	// read input from standard input and create tuples
	inputTuples := make([]worker.Tuple, 0)
	worker.ReadTupleStream(os.Stdin, func(tuple worker.Tuple, index int) {
		inputTuples = append(inputTuples, tuple)
	})

	// get standard output
	stdout := os.NewFile(uintptr(syscall.Stdout), "/dev/stdout")

	// iterate over input tuples
	for _, tuple := range inputTuples {
		words := strings.Fields(tuple.Slice[0])
		// iterate over words
		for _, word := range words {
			// emit each word in a new tuple
			outTuple := worker.Tuple{[]string{strings.ToLower(word), "1"}}
			stdout.Write(outTuple.SerializeTuple(0))
			stdout.Write([]byte{'\n'})
		}
	}
}
