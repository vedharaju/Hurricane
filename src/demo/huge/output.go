package main

import "os"
import "syscall"
import "worker"
import "strconv"

func main() {

	file, err := os.Create("output.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	count := make(map[string]int)

	worker.ReadTupleStream(os.Stdin, func(tuple worker.Tuple, index int) {
		switch index {
		case 0:
			num, _ := strconv.Atoi(tuple.Slice[1])
			count[tuple.Slice[0]] += num
		case 1:
			num, _ := strconv.Atoi(tuple.Slice[1])
			count[tuple.Slice[0]] += num
		case 2:
			num, _ := strconv.Atoi(tuple.Slice[1])
			count[tuple.Slice[0]] -= num
		}
	})

	stdout := os.NewFile(uintptr(syscall.Stdout), "/dev/stdout")

	for key, value := range count {
		file.Write([]byte(key + ", " + strconv.Itoa(value) + "\n"))
		outTuple := worker.Tuple{[]string{key, strconv.Itoa(value)}}
		stdout.Write(outTuple.SerializeTuple(0))
		stdout.Write([]byte{'\n'})
	}
}
