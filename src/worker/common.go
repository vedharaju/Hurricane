package worker

// Common data structures for the worker nodes
import "encoding/json"
import "io"
import "bytes"
import "os"
import "hash/fnv"
import "log"
import "os/exec"
import "strings"
import "client"
import "path"

type WorkerInternalClerk struct {
	// (host:port) information
	hostname string
}

func MakeWorkerInternalClerk(hostname string) *WorkerInternalClerk {
	ck := new(WorkerInternalClerk)
	ck.hostname = hostname
	return ck
}

func (ck *WorkerInternalClerk) GetTuples(args *GetTuplesArgs, numRetries int) *GetTuplesReply {
	for i := 0; i < numRetries; i++ {
		reply := GetTuplesReply{}
		ok := client.CallRPC(ck.hostname, "Worker.GetTuples", args, &reply)
		if ok {
			return &reply
		}
	}
	return nil
}

func (ck *WorkerInternalClerk) GetSegment(args *GetSegmentArgs, numRetries int) *GetSegmentReply {
	for i := 0; i < numRetries; i++ {
		reply := GetSegmentReply{}
		ok := client.CallRPC(ck.hostname, "Worker.GetSegment", args, &reply)
		if ok {
			return &reply
		}
	}
	return nil
}

type GetTuplesArgs struct {
	SegmentId      int64
	PartitionIndex int
	WorkerId       int64
}

type GetTuplesReply struct {
	Tuples []Tuple
	Err    client.Err
}

type GetSegmentArgs struct {
	SegmentId int64
	WorkerId  int64
}

type GetSegmentReply struct {
	Segment *Segment
	Err     client.Err
}

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
		for i := 0; i < n; i++ {
			if buff[i] == '\n' {
				line = append(line, buff[start:i]...)
				start = i + 1
				tuple, index := DeserializeTuple(line)
				callback(tuple, index)
				line = line[0:0]
			}
		}
		line = append(line, buff[start:n]...)
	}
}

func TupleToPartition(tuple Tuple, indices []int, parts int) int {
	var buffer bytes.Buffer

	for _, i := range indices {
		buffer.WriteString(tuple.Slice[i])
	}

	h := fnv.New32()
	io.WriteString(h, buffer.String())
	return int(h.Sum32() % uint32(parts))
}

type Segment struct {
	Partitions [][]Tuple
	Id         int64
}

func MakeSegment(tuples []Tuple, indices []int, parts int) *Segment {
	var segment Segment
	segment.Partitions = make([][]Tuple, parts)

	for i, _ := range segment.Partitions {
		segment.Partitions[i] = make([]Tuple, 0)
	}

	// partition the tuples based on the specified indices
	for _, tuple := range tuples {
		partition := TupleToPartition(tuple, indices, parts)
		segment.Partitions[partition] = append(segment.Partitions[partition], tuple)
	}
	return &segment
}

func preprocessCommand(command string) string {
	gopath := os.Getenv("GOPATH")
	base := path.Clean(gopath)
	return strings.Replace(command, "@", base, -1)
}

// Execute a UDF command that accepts zero or more input lists of tuples, and
// returns one output list of tuples. This function blocks until the UDF is
// done executing.
func runUDF(command string, inputTuples map[int][]Tuple) []Tuple {
	// spawn the external process
	splits := strings.Split(command, " ")
	client.Debug(preprocessCommand(splits[0]))
	cmd := exec.Command(preprocessCommand(splits[0]), splits[1:]...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Panic(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Panic(err)
	}
	cmd.Start()

	// write tuples to standard input on a background goroutine
	go func() {
		for index, tupleList := range inputTuples {
			for _, tuple := range tupleList {
				stdin.Write(tuple.SerializeTuple(index))
				stdin.Write([]byte{'\n'})
			}
		}
		stdin.Close()
	}()

	// read from standard output to get the output tuples
	outputTuples := make([]Tuple, 0)
	ReadTupleStream(stdout, func(tuple Tuple, index int) {
		outputTuples = append(outputTuples, tuple)
	})

	return outputTuples
}
