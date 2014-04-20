package worker

// Common data structures for the worker nodes
import "encoding/json"

type Tuple struct {
  Slice []string
}

func MakeTuple(length int) Tuple {
  tuple := Tuple{}
  tuple.Slice = make([]string, length)
  return tuple
}

func (tuple Tuple) SerializeTuple() []byte {
  bytes, err := json.Marshal(tuple.Slice)
  if err != nil {
    panic(err.Error())
  }
  return bytes
}

func DeserializeTuple(input []byte) Tuple {
  var slice []string
  err := json.Unmarshal(input, &slice)
  if err != nil {
    panic(err.Error())
  }
  return Tuple{slice}
}
