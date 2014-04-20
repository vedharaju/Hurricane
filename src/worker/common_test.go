package worker;

import "testing"
import "reflect"

func TestSerializeDeserialize(t *testing.T) {
  tuple1 := MakeTuple(2)
  tuple1.Slice[0] = "MOO"
  tuple1.Slice[1] = "abc"

  tuple2 := DeserializeTuple(tuple1.SerializeTuple())

  if !reflect.DeepEqual(tuple1, tuple2) {
    t.Errorf("Failure %s != %s", tuple1, tuple2)
  }
}
