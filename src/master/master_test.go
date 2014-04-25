package master

import "testing"
import "runtime"
import "strconv"
import "os"
import "fmt"

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

func cleanup(ma []*Master) {
	for i := 0; i < len(ma); i++ {
		if ma[i] != nil {
			ma[i].kill()
		}
	}
}

func TestBasicPing(t *testing.T) {
	runtime.GOMAXPROCS(2)

	masterhost := port("master", 1)
	StartServer(masterhost)

	workerhost := port("worker", 2)
	worker := MakeClerk(workerhost, masterhost)

	if ok := worker.Ping(); !ok {
		t.Fatalf("Could not ping master")
	}

	fmt.Printf("  ... Passed\n")
}
