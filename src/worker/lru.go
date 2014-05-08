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
	exists   map[int64]bool
	segments map[int64]*list.Element
	lru      *list.List
	capacity int
	filepath string
}

func NewLRU(capacity int, wid int64) *LRU {
	if capacity < 1 {
		panic("capacity < 1")
	}
	c := &LRU{exists: make(map[int64]bool), segments: make(map[int64]*list.Element), lru: list.New(), capacity: capacity, filepath: "/src/segments/segment" + strconv.FormatInt(wid, 10) + "_"}
	return c
}

func (c *LRU) Length() int {
	return c.lru.Len()
}

func (c *LRU) evictAsNecessary() {
	for c.Length() > c.capacity {
		segment := c.lru.Remove(c.lru.Back()).(*Segment)
		delete(c.segments, segment.Id) //delete this segment from the map
		c.moveToDisk(segment)
	}
}

func (c *LRU) Insert(key int64, segment *Segment) {
	el := c.lru.PushFront(segment)
	c.segments[key] = el
	c.exists[key] = true
	c.evictAsNecessary()
}

func (c *LRU) Get(key int64) *Segment { //change to return *Segment
	s, cached := c.segments[key]
	if cached {
		c.lru.MoveToFront(s)
		return s.Value.(*Segment)
	} else if c.exists[key] {
		segment := c.findSegmentFromFile(key)

		c.removeFile(key)
		c.Insert(key, segment)
		return segment
	} else {
		return nil
	}
}

func (c *LRU) findSegmentFromFile(key int64) *Segment {
	gopath := os.Getenv("GOPATH")
	segment_file := path.Join(gopath, c.filepath+strconv.FormatInt(key, 10))
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

func (c *LRU) removeFile(key int64) error {
	gopath := os.Getenv("GOPATH")
	segment_file := path.Join(gopath, c.filepath+strconv.FormatInt(key, 10))
	err := os.Remove(segment_file)
	if err != nil {
		panic(err)
	}
	return nil
}

func (c *LRU) moveToDisk(segment *Segment) error {
	gopath := os.Getenv("GOPATH")
	segment_file := path.Join(gopath, c.filepath+strconv.FormatInt(segment.Id, 10))
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
