package main

// Common data structures for the worker nodes
import "encoding/json"
import "fmt"

type Tuple struct {
  slice []string
}

func MakeTuple(length int) *Tuple {
  tuple := &Tuple{}
  tuple.slice = make([]string, length)
  return tuple
}

func SerializeTuple(tuple *Tuple) []byte {
  bytes, err := json.Marshal(tuple.slice)
  if err != nil {
    panic(err.Error())
  }
  return bytes
}

func DeserializeTuple(input []byte) *Tuple {
  tuple := MakeTuple(0)
  err := json.Unmarshal(input, &tuple.slice)
  if err != nil {
    panic(err.Error())
  }
  return tuple
}
