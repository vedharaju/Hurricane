package main

import "os"
import "syscall"
import "worker"
import "strconv"

func main() {

	// read from standard input to get the input tuples
	inputTuples := make([]worker.Tuple, 0)
	worker.ReadTupleStream(os.Stdin, func(tuple worker.Tuple, index int) {
		inputTuples = append(inputTuples, tuple)
	})

	stdout := os.NewFile(uintptr(syscall.Stdout), "/dev/stdout")

	counts := make(map[string]int)
	for _, tuple := range inputTuples {
		word := tuple.Slice[0]

		if count, ok := counts[word]; ok {
			counts[word] = count + 1
		} else {
			counts[word] = 1
		}
	}

	for word, count := range counts {
		outTuple := worker.Tuple{[]string{word, strconv.Itoa(count)}}
		stdout.Write(outTuple.SerializeTuple(0))
		stdout.Write([]byte{'\n'})
	}
}
