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

  count := 0
  for _, _ = range inputTuples {
    // word := tuple.Slice[0]
    count += 1
  }

  outTuple := worker.Tuple{[]string{"error", strconv.Itoa(count)}}
  stdout.Write(outTuple.SerializeTuple(0))
  stdout.Write([]byte{'\n'})
}
