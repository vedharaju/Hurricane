package main

import "os"
import "worker"

func main() {

	file, err := os.Create("output.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	worker.ReadTupleStream(os.Stdin, func(tuple worker.Tuple, index int) {
		file.Write([]byte(tuple.Slice[0] + ", " + tuple.Slice[1] + "\n"))
	})
}
