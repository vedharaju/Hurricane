package worker

import "testing"
import "runtime"
import "strconv"
import "os"
import "fmt"
import "master"

func port(tag string, host int) string {
	s := "/var/tmp/hurricane-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "w-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

func cleanup(wa []*Worker) {
	for i := 0; i < len(wa); i++ {
		if wa[i] != nil {
			wa[i].kill()
		}
	}
}

func TestBasicStartWorker(t *testing.T) {
	runtime.GOMAXPROCS(2)

	fmt.Printf("Test: Worker starts up without issues")
	masterhost := port("master", 1)
	master.StartServer(masterhost)

	workerhost := port("worker", 2)
	StartServer(workerhost, masterhost)

	fmt.Printf("  ... Passed\n")
}
