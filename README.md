Hurricane
=========

Hurricane will be designed in the style of Spark Streaming. At a high level, Hurricane performs chained Map-Reduce jobs in small batches at a high frequency (about once per second). Intermediate results are stored in RDDs. Fault tolerance is achieved the in same style as Spark.

There is a single, unreplicated master node and several worker nodes. The master tracks all RDDs and their dependencies. Every second, the master launches the tasks to generate the set of RDDs for the next batch. The system reads its data from a persistent, streaming, log-file storage system such as Kafka. For the purposes of this project, we will read data from a fake Kafka broker. Outputs will be written to text files.
