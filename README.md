# Hurricane

Hurricane was designed and implemented as part of MIT's 6.824 distributed systems class.  The system is highly inspired by storm at spark streaming. Hurricane is a language agnostic, realtime, distributed data processing system.  It was written and tested in Go.

Hurricane performs well.  We were able to process between 50k and 150k records per second per node on an AWS setup.

MIT 6.824 Distributed Systems [Final paper](https://docs.google.com/document/d/1o87DJr37dUiRn70ZPrEBgGqeSDmkD8M4MLA63Qj2uys).

[Performance Testing](https://www.youtube.com/watch?v=FmS21saPdkY) on AWS.

## Simple Getting started
Hurricane processes data through user defined jobs.  These jobs can be written in any language as long as they can read from stdin and output to stdout.  Jobs should be precompiled as to reduce overhead during computation. Precompile user defined functions:
```console
sh build_udfs.sh
```

The master node persists datastructures to disk and keeps relations in a postgresql database.  The database must be initialized before use.  This will create tables and clear any existing data.  Initialize the database:
``` console
go run init_database.go
```

The master needs to load the workflow into memory in order to know what jobs exist and how the data should propogate through the system.  Load the workflow:
```console
go run load_workflow.go src/demo/wordcount_go/wc_workflow
```

Once everything is set up, the next step is to start the master and workers.  Additional workers may be started for increased performance and fault tolerance.  
```console
go run start_master.go localhost:1234

go run start_worker.go localhost:1235 localhost:1234
```

## Example: Wordcount
Hurricane can be used to process data.  In this example, we will show how a simple [wordcount program](src/demo/wordcount_go) can be written.  Other examples such as grep exist.  [This](src/demo/huge) is an example which keeps a running sum of errors found in a kafka log over the past 30 seconds.

### Define a workflow.
The heart of the system lies in the workflow.  A workflow is a directed graph of jobs to be run over the data.  Jobs can manipulate and change the data and pass them onto other jobs.  Each job will be run on a different worker as workers become available to process more data.  This is an example of a wordcount workflow.
```
d=1000

JOBS

A: @/src/demo/wordcount_go/wordcount_input.udf ;; r=true & p=(0) & w=1 & b=1
B: @/src/demo/wordcount_go/wordcount_map.udf ;; r=true & p=(0) & w=1 & b=10
C: @/src/demo/wordcount_go/wordcount_reduce.udf ;; r=true & p=(0) & w=2 & b=1 & c=1
D: @/src/demo/wordcount_go/wordcount_output.udf ;; r=true & p=(0) & w=1 & b=1

WORKFLOW

A -> B
B -> C
C -> D

```

There are 4 jobs in this linear workflow.  Together they read in a file, computer a MR wordcount over the data, and output to disk.

 - input: Reads in a file and converts it to a hurricane RDD.
 - map: Tokenize words into a key-value RDD.
 - reduce: Group together key-value pairs into a new RDD.
 - output: Write data to disk (could be a NFS).

### Write each job in the workflow as a UDF
Each job in a workflow is a user defined function (UDF).  This is an example of the map job for a wordcount program written in Go.
```go
func main() {

  // read input from standard input and create tuples
  inputTuples := make([]worker.Tuple, 0)
  worker.ReadTupleStream(os.Stdin, func(tuple worker.Tuple, index int) {
    inputTuples = append(inputTuples, tuple)
  })

  // get standard output
  stdout := os.NewFile(uintptr(syscall.Stdout), "/dev/stdout")

  // iterate over input tuples
  for _, tuple := range inputTuples {
    words := strings.Fields(tuple.Slice[0])
    // iterate over words
    for _, word := range words {
      // emit each word in a new tuple
      outTuple := worker.Tuple{[]string{strings.ToLower(word), "1"}}
      stdout.Write(outTuple.SerializeTuple(0))
      stdout.Write([]byte{'\n'})
    }
  }
}
```
The input tuples for each job are read from stdin.  Output tuples should be written to stdout.  This allows UDFs to be written in any language.  

## Resources

**Project proposal**: https://docs.google.com/document/d/1ts-cprYUZvTfWuIF8rb6NJJ7GF61RfSMyqOJ2612qOE

**Design Doc**: https://docs.google.com/document/d/1SHegRAhPv6XI5L4o1y2YCabXwOlK133KVEsohwCfXUA
