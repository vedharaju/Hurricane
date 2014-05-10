package worker

import "testing"
import "fmt"
import "os"

func TestLRU(t *testing.T) {
	gopath := os.Getenv("GOPATH")
	if _, err := os.Stat(gopath + "/src/segments"); err != nil {
		if os.IsNotExist(err) {
			os.Mkdir(gopath+"/src/segments", 0777)
		} else {
			panic(err)
		}
	}
	tuple1 := Tuple{Slice: []string{"Vedha", "Vikas", "Jeffrey", "Zack"}}
	tuple2 := Tuple{Slice: []string{"Vivek", "Anuhya", "Esha"}}
	tuple3 := Tuple{Slice: []string{"Christina", "Keerti"}}
	tuple4 := Tuple{Slice: []string{"Suganya", "Arooshi"}}

	var segment1 Segment
	segment1.Partitions = make([][]Tuple, 2)
	segment1.Partitions[0] = []Tuple{tuple1, tuple2}
	segment1.Partitions[1] = []Tuple{tuple3, tuple4}
	segment1.Id = 1234

	var segment2 Segment
	segment2.Partitions = make([][]Tuple, 2)
	segment2.Partitions[0] = []Tuple{tuple1, tuple3}
	segment2.Partitions[1] = []Tuple{tuple2, tuple4}
	segment2.Id = 1111

	lru := NewLRU(1, 4)
	lru.Insert(1234, &segment1)
	lru.Insert(1111, &segment2)
	s := lru.Get(1234)
	s2 := lru.Get(1111)
	fmt.Println("Here's what I got", s)
	fmt.Println(s2)
	fmt.Println("Length", lru.Length())
}
