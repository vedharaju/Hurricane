package main

import "os"
import "syscall"
import "time"
import "worker"

func main() {
	stdout := os.NewFile(uintptr(syscall.Stdout), "/dev/stdout")

	t := time.Now()
	strings := []string{"Zack", "Jeff", "Vedha", "Vikas", "Zack"}

	for _, str := range strings {
		tuple := worker.Tuple{[]string{str, t.String()}}
		stdout.Write(tuple.SerializeTuple(0))
		stdout.Write([]byte{'\n'})
	}
}
