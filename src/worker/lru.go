package worker

//import "bufio"
//import "bytes"
import "container/list"
import "strconv"
import "encoding/gob"

//import "io/ioutil"
import "os"
import "log"
import "path"

//import "fmt"

type LRU struct {
	segments map[int64]*list.Element
	lru      *list.List
	capacity int
}

func NewLRU(capacity int) *LRU {
	if capacity < 1 {
		panic("capacity < 1")
	}
	c := &LRU{segments: make(map[int64]*list.Element), lru: list.New(), capacity: capacity}
	return c
}

func (c *LRU) Length() int {
	return c.lru.Len()
}

func (c *LRU) evictAsNecessary() {
	for c.Length() > c.capacity {
		segment := c.lru.Remove(c.lru.Back()).(*Segment)
		delete(c.segments, segment.Id) //delete this segment from the map
		moveToDisk(segment)
	}
}

func (c *LRU) Insert(key int64, segment *Segment) {
	el := c.lru.PushFront(segment)
	c.segments[key] = el
	c.evictAsNecessary()
}

func (c *LRU) Get(key int64) *Segment { //change to return *Segment
	s, cached := c.segments[key]
	if cached {
		c.lru.MoveToFront(s)
		return s.Value.(*Segment)
	} else {
		segment := findSegmentFromFile(key)

		removeFile(key)
		c.Insert(key, segment)
		return segment
	}
}

func findSegmentFromFile(key int64) *Segment {
	gopath := os.Getenv("GOPATH")
	segment_file := path.Join(gopath, "/src/segments/segment_"+strconv.FormatInt(key, 10))
	file, err := os.Open(segment_file) // For read access.
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}

	var decodedSegment *Segment
	d := gob.NewDecoder(file)

	// Decoding the serialized data
	err = d.Decode(&decodedSegment)
	if err != nil {
		panic(err)
	}

	return decodedSegment
}

func removeFile(key int64) error {
	gopath := os.Getenv("GOPATH")
	segment_file := path.Join(gopath, "/src/segments/segment_"+strconv.FormatInt(key, 10))
	err := os.Remove(segment_file)
	if err != nil {
		panic(err)
	}
	return nil
}

func moveToDisk(segment *Segment) error {
	gopath := os.Getenv("GOPATH")
	segment_file := path.Join(gopath, "/src/segments/segment_"+strconv.FormatInt(segment.Id, 10))
	file, err := os.Create(segment_file)
	defer file.Close()
	if err != nil {
		panic(err)
	}

	e := gob.NewEncoder(file)

	// serializing segment
	err = e.Encode(segment)
	if err != nil {
		panic(err)
	}
	return nil
}
