package main

import "os"
import "syscall"
import "worker"
import "time"
import "strconv"
import "math/rand"

// go run input.go start_time duration number_of_log random_seed
// go run input.go now 60 2000 5
func main() {
	d, _ := strconv.Atoi(os.Args[2])
	duration := time.Second * time.Duration(d)

	startTime := time.Now() // os.Args[1]

	num, _ := strconv.Atoi(os.Args[3])

	seed, _ := strconv.ParseInt(os.Args[4], 10, 64)

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
