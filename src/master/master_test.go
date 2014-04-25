package master

import "testing"
import "strconv"
import "fmt"

func port(url string, port int) string {
	return url + ":" + strconv.Itoa(port)
}

func TestBasicPing(t *testing.T) {
	// setup test database
	hd := GetTestDbConnection()
	ResetDb(hd)
	CreateTables(hd)

	masterhost := port("localhost", 58293)
	StartServer(masterhost, hd)

	workerhost := port("localhost", 13243)
	worker := MakeClerk(workerhost, masterhost)

	// register and ping 1 worker
	if id := worker.Register(false); id < 0 {
		t.Fatalf("Worker1 could not register with master")
	}

	if ok := worker.Ping(false); ok != OK {
		t.Fatalf("Worker1 could not ping master, got %v", ok)
	}

	// register and ping a new worker at the same address
	worker2 := MakeClerk(workerhost, masterhost)
	if id := worker2.Register(false); id < 0 {
		t.Fatalf("Worker2 could not register with master")
	}

	if ok := worker2.Ping(false); ok != OK {
		t.Fatalf("Worker1 could not ping master, got %v", ok)
	}

	// worker 1 should not be able to ping, since it was replaced by
	// worker 2
	if ok := worker.Ping(false); ok != RESET {
		t.Fatalf("Worker1 should have received RESET, got %v", ok)
	}

	fmt.Printf("  ... Passed\n")
}
