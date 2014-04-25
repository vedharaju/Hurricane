package worker

// Common data structures for the worker nodes
import "encoding/json"
import "io"

type Tuple struct {
  Slice []string
}

type TupleFunc func(Tuple)

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

func ReadTupleStream(reader io.Reader, callback TupleFunc) {
  buff := make([]byte, 4096)
  line := make([]byte, 0, 4096)
  for {
    n, err := reader.Read(buff)
    if err != nil {
      return
    }
    start := 0
    for i:=0; i<n; i++ {
      if buff[i] == '\n' {
        line = append(line, buff[start:i]...)
        start = i+1
        callback(DeserializeTuple(line))
        line = line[0:0]
      }
    }
    line = append(line, buff[start:n]...)
  }
}

type Segment struct {
  Tuples []Tuple
}

func MakeSegment(tuples []Tuple) Segment {
  var segment Segment
  segment.Tuples = tuples
  return segment
}
