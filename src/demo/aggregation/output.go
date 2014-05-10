package main

import "os"
import "worker"
import "time"

func main() {

  file, err := os.Open("output.txt" + time.Now().String())
  if err != nil {
    file, err = os.Create("output.txt" + time.Now().String())
    if err != nil {
      panic(err)
    }
  }
  defer file.Close()

  worker.ReadTupleStream(os.Stdin, func(tuple worker.Tuple, index int) {
    file.Write([]byte(tuple.SerializeTuple(index)))
    file.Write([]byte(tuple.Slice[0] + ", " + tuple.Slice[1] + "\n"))
  })
}
