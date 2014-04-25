package worker

// Common data structures for the worker nodes
import "encoding/json"
import "io"

type Tuple struct {
  Slice []string
}

type TupleFunc func(Tuple, int)

func MakeTuple(length int) Tuple {
  tuple := Tuple{}
  tuple.Slice = make([]string, length)
  return tuple
}

// Serialize a tuple into a json blob of the following form:
//   [["word1", "word2", "word3"], 4]
// where the words are from tuple.Slice, and the number is the index
func (tuple Tuple) SerializeTuple(index int) []byte {
  bytes, err := json.Marshal([]interface{}{tuple.Slice, index})
  if err != nil {
    panic(err.Error())
  }
  return bytes
}

// Opposite of SerializeTuple
func DeserializeTuple(input []byte) (Tuple, int) {
  var output []interface{}
  err := json.Unmarshal(input, &output)
  if err != nil {
    panic(err.Error())
  }
  outputList := output[0].([]interface{})
  slice := make([]string, len(outputList))
  for i := range slice {
    slice[i] = outputList[i].(string)
  }
  return Tuple{slice}, int(output[1].(float64))
}

// Read a stream of serialized tuples, deserialize those tuples,
// and execute the callback function for each one of those tuples.
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
        tuple, index := DeserializeTuple(line)
        callback(tuple, index)
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
