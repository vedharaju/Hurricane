package main

import "os"
import "syscall"
import "worker"
import "strconv"

func main() {

	count := make(map[string]int)

	worker.ReadTupleStream(os.Stdin, func(tuple worker.Tuple, index int) {
		count[tuple.Slice[0]] += 1
	})

	stdout := os.NewFile(uintptr(syscall.Stdout), "/dev/stdout")

	for key, value := range count {
		outTuple := worker.Tuple{[]string{key, strconv.Itoa(value)}}
		stdout.Write(outTuple.SerializeTuple(0))
		stdout.Write([]byte{'\n'})
	}
}
