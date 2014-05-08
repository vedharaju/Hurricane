package main

import "os"
import "syscall"
import "worker"
import "strings"

/*import "fmt"*/

func main() {

  // read from standard input to get the input tuples
  inputTuples := make([]worker.Tuple, 0)
  worker.ReadTupleStream(os.Stdin, func(tuple worker.Tuple, index int) {
    inputTuples = append(inputTuples, tuple)
  })

  stdout := os.NewFile(uintptr(syscall.Stdout), "/dev/stdout")

  for _, tuple := range inputTuples {
    words := strings.Fields(tuple.Slice[0])
    for _, word := range words {
      word = strings.ToLower(word)
      if word == "error" {
        outTuple := worker.Tuple{[]string{word, "1"}}
        stdout.Write(outTuple.SerializeTuple(0))
        stdout.Write([]byte{'\n'})
      }
    }
  }
}
