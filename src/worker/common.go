package worker

// Common data structures for the worker nodes
import "encoding/json"

type Tuple struct {
  Slice []string
}

func MakeTuple(length int) *Tuple {
  tuple := &Tuple{}
  tuple.Slice = make([]string, length)
  return tuple
}

func (tuple *Tuple) Serialize() []byte {
  bytes, err := json.Marshal(tuple.Slice)
  if err != nil {
    panic(err.Error())
  }
  return bytes
}

func (tuple *Tuple) Deserialize(input []byte) {
  err := json.Unmarshal(input, &tuple.Slice)
  if err != nil {
    panic(err.Error())
  }
}
