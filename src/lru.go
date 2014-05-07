package lru

import "container/list"
import "strconv"

type LRU struct{
  segments map[int64]*list.Element
  lru list.List
  capacity int 
}

func New(c int) *LRU {
    if capacity < 1 {
        panic("capacity < 1")
    }
    c := &LRU{segment: make(map[int64]*list.Element), lru: list.New(), capacity: c}
    return c
}

func (c *LRU) length() int {
  return c.lru.Len()
}


func (c *LRU) evictAsNecessary() {
  for c.length() > c.capacity {
    segment := c.lru.Remove(c.lru.Back)
    // del segments[] TODO: add id to segment and then delete this from the map
    moveToDisk(segment)
  }
}

func (c *LRU) Insert(key int64, segment *Segment) {
  el := c.lru.PushToFront(segment)
  c.segments[key] = el
  c.evictAsNecessary()
}

func (c *LRU) Get(key int64) interface{} {  //change to return *Segment
  s, cached := c.segments[key]
  if cached {
    c.lru.MoveToFront(s)
    return s.Value
  } else {
    segment := findSegmentFromFile(key)
    removeFile(key)
    c.Insert(key,segment)
    return segment
  }
}

func findSegmentFromFile(key int64) (*Segment, error){
  segment_file := "/src/segments/segment_" + strconv.FormatInt(key, 10)
  // TODO: open file
  // TODO: deserialize segment from file and return it
}

func removeFile(key int64) error {
  segment_file := "/src/segments/segment_" + strconv.FormatInt(key, 10)
  os.Remove(segment_file)
}

func moveToDisk(segment *Segment) error {
  segment_file := "/src/segments/segment_" // TODO: get segment_id from segment and find correct file
  // os.Open() TODO: open file
  // TODO: serialize segment
  // write serialized segment to file
}
