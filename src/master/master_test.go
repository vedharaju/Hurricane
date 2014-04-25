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

	const nservers = 1
	var ma []*Master = make([]*Master, nservers)
	var mh []string = make([]string, nservers)
	defer cleanup(ma)

	for i := 0; i < nservers; i++ {
		mh[i] = port("basic", i)
	}
	for i := 0; i < nservers; i++ {
		ma[i] = StartServer(mh[i])
	}

	ck := MakeClerk(mh)
	var cka [nservers]*Clerk
	for i := 0; i < nservers; i++ {
		cka[i] = MakeClerk([]string{mh[i]})
	}

	fmt.Printf("Test: Basic ping ...\n")

	ok := ck.Ping(mh[0])
	if !ok {
		t.Fatalf("Ping failure")
	}

	fmt.Printf("  ... Passed\n")
}
