package worker

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

func cleanup(wa []*Worker) {
	for i := 0; i < len(wa); i++ {
		if wa[i] != nil {
			wa[i].kill()
		}
	}
}

func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const nservers = 3
	var wa []*Worker = make([]*Worker, nservers)
	var wh []string = make([]string, nservers)
	defer cleanup(wa)

	for i := 0; i < nservers; i++ {
		wh[i] = port("basic", i)
	}
	for i := 0; i < nservers; i++ {
		wa[i] = StartServer(wh, i)
	}

	ck := MakeClerk(wh)
	var cka [nservers]*Clerk
	for i := 0; i < nservers; i++ {
		cka[i] = MakeClerk([]string{wh[i]})
	}

	fmt.Printf("Test: Basic ping ...\n")

	pr := ck.Ping(wh[0])
	if !pr {
		t.Fatalf("Ping failure")
	}

	fmt.Printf("  ... Passed\n")
}
