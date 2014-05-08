Hurricane
=========

Hurricane will be designed in the style of Spark Streaming. At a high level, Hurricane performs chained Map-Reduce jobs in small batches at a high frequency (about once per second). Intermediate results are stored in RDDs. Fault tolerance is achieved the in same style as Spark.

There is a single, unreplicated master node and several worker nodes. The master tracks all RDDs and their dependencies. Every second, the master launches the tasks to generate the set of RDDs for the next batch. The system reads its data from a persistent, streaming, log-file storage system such as Kafka. For the purposes of this project, we will read data from a fake Kafka broker. Outputs will be written to text files.

Getting started
---------------
Precompile user defined functions

`./build_udfs.sh`

Initialize the database

`go run init_database.go`

Load the workflow

`go run load_workflow.go src/demo/wordcount_go/wc_workflow`

Start a master

`go run start_master.go localhost:1234`

Start a worker (additional workers if necessary)

`go run start_worker.go localhost:1235 localhost:1234`


Resources
---------
**Project proposal**: https://docs.google.com/document/d/1ts-cprYUZvTfWuIF8rb6NJJ7GF61RfSMyqOJ2612qOE

**Design Doc**: https://docs.google.com/document/d/1SHegRAhPv6XI5L4o1y2YCabXwOlK133KVEsohwCfXUA
