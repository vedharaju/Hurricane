package master

import "testing"
import "runtime"
import "strconv"
import "fmt"

func port(url string, port int) string {
	return url + ":" + strconv.Itoa(port)
}

func TestBasicPing(t *testing.T) {
	runtime.GOMAXPROCS(2)

	// setup test database
	hd := GetTestDbConnection()
	ResetDb(hd)
	CreateTables(hd)

	masterhost := port("localhost", 58293)
	StartServer(masterhost, hd)

	workerhost := port("localhost", 13243)
	worker := MakeClerk(workerhost, masterhost)

	if ok := worker.Ping(); !ok {
		t.Fatalf("Could not ping master")
	}

	fmt.Printf("  ... Passed\n")
}
