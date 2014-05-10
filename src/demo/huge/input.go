package main

import "os"
import "syscall"
import "worker"
import "strconv"

const (
	ERROR = "error"
	PASS  = "pass"
)

// go run input.go start_time duration number_of_log
// go run input.go now 60 2000 5
func main() {
	var value string

	d, _ := strconv.Atoi(os.Args[2])
	start, _ := strconv.Atoi(os.Args[1])
	num, _ := strconv.Atoi(os.Args[3])

	max := d * num

	stdout := os.NewFile(uintptr(syscall.Stdout), "/dev/stdout")

	for i := 0; i < max; i++ {
		if i%num == 0 {
			value = ERROR
		} else {
			value = PASS
		}
		s := []string{value, strconv.Itoa(i + start)}
		tuple := worker.Tuple{s}
		stdout.Write(tuple.SerializeTuple(0))
		stdout.Write([]byte{'\n'})
	}
}
