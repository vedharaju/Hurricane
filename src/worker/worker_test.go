package worker

import "testing"
import "runtime"
import "strconv"
import "fmt"
import "master"

func port(url string, port int) string {
	return url + ":" + strconv.Itoa(port)
}

func TestBasicStartWorker(t *testing.T) {
	runtime.GOMAXPROCS(2)

	fmt.Printf("Test: Worker starts up without issues\n")
	hd := master.GetTestDbConnection()
	fmt.Printf("got Db connection")
	master.ResetDb(hd)
	master.CreateTables(hd)

	masterhost := port("localhost", 1324)
	master.StartServer(masterhost, hd)

	workerhost := port("localhost", 2222)
	StartServer(workerhost, masterhost)

	fmt.Printf("  ... Passed\n")
}
