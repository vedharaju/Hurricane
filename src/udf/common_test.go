package udf

import "testing"
import "worker"
import "reflect"

// A UDF with two inputs and a unity function ("tee") should
// concatenate the input tuples into the output
func TestSimpleUdf(t *testing.T) {
	t1 := worker.MakeTuple(1)
	t2 := worker.MakeTuple(1)
	t1.Slice[0] = "Tuple1"
	t2.Slice[0] = "Tuple2"
	in1 := []worker.Tuple{t1}
	in2 := []worker.Tuple{t2}
	output := runUDF("tee", in1, in2)

	expected := []worker.Tuple{t1, t2}

	if !reflect.DeepEqual(output, expected) {
		t.Errorf("Failure %s != %s", output, expected)
	}
}
