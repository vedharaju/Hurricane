package main

import "os"
import "syscall"
import "worker"
import "strconv"
import "time"

func main() {

  file, err := os.Open("input-combine-" + time.Now().String())
  if err != nil {
    file, err = os.Create("input-combine-" + time.Now().String())
    if err != nil {
      panic(err)
    }
  }
  defer file.Close()


  // read from standard input to get the input tuples
  newTuples := make([]worker.Tuple, 0)
//  oldTuples := make([]worker.Tuple, 0)*/
  totalTuples := make([]worker.Tuple, 0)

  worker.ReadTupleStream(os.Stdin, func(tuple worker.Tuple, index int) {
    file.Write([]byte(tuple.SerializeTuple(index)))

    switch(index) {
      case 1:
        newTuples = append(newTuples, tuple)
//    case 1:
  //      oldTuples = append(oldTuples, tuple)*/
      case 0:
        totalTuples = append(totalTuples, tuple)
    }
  })

  stdout := os.NewFile(uintptr(syscall.Stdout), "/dev/stdout")

  counts := make(map[string]int)
  for _, tuple := range totalTuples {
    word := tuple.Slice[0]
    prev, _ := strconv.Atoi(tuple.Slice[1])

    if count, ok := counts[word]; ok {
      counts[word] = count + prev
    } else {
      counts[word] = prev
    }
  }

  for _, tuple := range newTuples {
    word := tuple.Slice[0]

    if count, ok := counts[word]; ok {
      counts[word] = count + 1
    } else {
      counts[word] = 1
    }
  }

//  for _, tuple := range oldTuples {
//    word := tuple.Slice[0]
//
  //  if count, ok := counts[word]; ok {
    //  counts[word] = count - 1
//    }
  //}

  file1, err := os.Open("prod-combine-" + time.Now().String())
  if err != nil {
    file1, err = os.Create("prod-combine-" + time.Now().String())
    if err != nil {
      panic(err)
    }
  }
  defer file1.Close()


  for word, count := range counts {
    outTuple := worker.Tuple{[]string{word, strconv.Itoa(count)}}
    file1.Write([]byte(outTuple.SerializeTuple(2)))

    stdout.Write(outTuple.SerializeTuple(2))
    stdout.Write([]byte{'\n'})
  }
}
