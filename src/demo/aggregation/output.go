package main

import "os"
import "worker"
import "time"

func main() {

	file, err := os.OpenFile("output.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	file.Write([]byte(time.Now().String() + "\n"))
	defer file.Close()

	worker.ReadTupleStream(os.Stdin, func(tuple worker.Tuple, index int) {
		file.Write([]byte(tuple.Slice[0] + ", " + tuple.Slice[1] + "\n"))
	})
}
