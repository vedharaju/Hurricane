package worker

import "testing"
import "reflect"
import "bytes"
import "fmt"
import "strconv"

func TestSerializeDeserialize(t *testing.T) {
	tuple1 := MakeTuple(2)
	tuple1.Slice[0] = "MOO"
	tuple1.Slice[1] = "abc"

	tuple2, index := DeserializeTuple(tuple1.SerializeTuple(1))

	if !reflect.DeepEqual(tuple1, tuple2) {
		t.Errorf("Failure %s != %s", tuple1, tuple2)
	}

	if index != 1 {
		t.Errorf("Failure, index %d != 1", index)
	}
}

func TestReadTupleStream(t *testing.T) {
	data := make([]byte, 0)
	oldsum := 0
	for i := 0; i < 5; i++ {
		tuple := MakeTuple(1)
		tuple.Slice[0] = strconv.Itoa(i)
		oldsum += i
		data = append(data, tuple.SerializeTuple(4)...)
		data = append(data, '\n')
	}

	newsum := 0
	ReadTupleStream(bytes.NewBuffer(data), func(tuple Tuple, index int) {
		num, _ := strconv.Atoi(tuple.Slice[0])
		newsum += num

		if index != 4 {
			t.Errorf("Failure index %d != 4", index)
		}
	})

	if oldsum != newsum {
		t.Errorf("Failure %d != %d", oldsum, newsum)
	}
}

func TestBasicSegment(t *testing.T) {
	//TODO: write better test

	fmt.Printf("Test: Basic Segment has tuples ...\n")

	//IMPORANT: for this test make sure values only repeat in same index
	// ie. tuple1.Slice[0] = "MOO"
	//     tuple1.Slice[1] = "MOO" is not good

	tuple1 := MakeTuple(2)
	tuple1.Slice[0] = "MOO"
	tuple1.Slice[1] = "abc"

	tuple2 := MakeTuple(2)
	tuple2.Slice[0] = "WOOF"
	tuple2.Slice[1] = "cde"

	tuple3 := MakeTuple(2)
	tuple3.Slice[0] = "WOOF"
	tuple3.Slice[1] = "abc"

	tuple4 := MakeTuple(2)
	tuple4.Slice[0] = "MEOW"
	tuple4.Slice[1] = "cde"

	tuples := []Tuple{tuple1, tuple2, tuple3, tuple4}

	counts := make(map[string]int)
	for _, tuple := range tuples {
		for _, v := range tuple.Slice {
			if count, ok := counts[v]; ok {
				counts[v] = count + 1
			} else {
				counts[v] = 1
			}
		}
	}

	indices := make([]int, 1)
	indices[0] = 0
	segment := MakeSegment(tuples, indices, 2)

	fmt.Printf("Checking that partitioning is correct ...\n")

	for _, partition := range segment.Partitions {
		partition_counts := make(map[string]int)

		for _, tuple := range partition {
			v := tuple.Slice[0]
			if count, ok := partition_counts[v]; ok {
				partition_counts[v] = count + 1
			} else {
				partition_counts[v] = 1
			}
		}

		for k, v := range partition_counts {
			if counts[k] != v {
				t.Errorf("Failure Partition Incorrect\n")
			}
		}
	}

	fmt.Printf("  ... Passed\n")
}

// A UDF with two inputs and a unity function ("tee") should
// concatenate the input tuples into the output
func TestSimpleUdf(t *testing.T) {
	t1 := MakeTuple(1)
	t2 := MakeTuple(1)
	t1.Slice[0] = "Tuple1"
	t2.Slice[0] = "Tuple2"
	in1 := []Tuple{t1}
	in2 := []Tuple{t2}
	output := runUDF("tee", in1, in2)

	expected := []Tuple{t1, t2}

	if !reflect.DeepEqual(output, expected) {
		t.Errorf("Failure %s != %s", output, expected)
	}
}
