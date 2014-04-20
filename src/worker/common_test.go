package worker;

import "testing"
import "reflect"
import "bytes"
import "strconv"

func TestSerializeDeserialize(t *testing.T) {
  tuple1 := MakeTuple(2)
  tuple1.Slice[0] = "MOO"
  tuple1.Slice[1] = "abc"

  tuple2 := DeserializeTuple(tuple1.SerializeTuple())

  if !reflect.DeepEqual(tuple1, tuple2) {
    t.Errorf("Failure %s != %s", tuple1, tuple2)
  }
}

func TestReadTupleStream(t *testing.T) {
  data := make([]byte,0)
  oldsum := 0
  for i:=0; i<5; i++ {
    tuple := MakeTuple(1)
    tuple.Slice[0] = strconv.Itoa(i)
    oldsum += i
    data = append(data, tuple.SerializeTuple()...)
    data = append(data, '\n')
  }

  newsum := 0
  ReadTupleStream(bytes.NewBuffer(data), func (t Tuple) {
    num, _ := strconv.Atoi(t.Slice[0])
    newsum += num
  })

  if oldsum != newsum {
    t.Errorf("Failure %d != %d", oldsum, newsum)
  }
}

