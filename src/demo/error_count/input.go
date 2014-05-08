package main

import "os"
import "syscall"
import "worker"
import "time"
import "math/rand"

// go run input.go start_time duration number_of_log random_seed
// go run input.go now 60 2000 5
func main() {
	duration := time.Second * 60

	startTime := time.Now()

	num  := 2000

	seed := int64(10)

	stdout := os.NewFile(uintptr(syscall.Stdout), "/dev/stdout")

	r := rand.New(rand.NewSource(seed))

	step := duration / time.Duration(num)

	for st := startTime; st.Before(startTime.Add(duration)); st = st.Add(step) {
		s := []string{"[" + st.String() + "]"}

		if r.Float32() < 0.1 {
			s = append(s, "error")
		} else {
			s = append(s, "pass")
		}

		tuple := worker.Tuple{s}

		stdout.Write(tuple.SerializeTuple(0))
		stdout.Write([]byte{'\n'})
	}
}
