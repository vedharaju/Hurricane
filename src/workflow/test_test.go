package workflow

import (
        "master"
        "testing"
        "log"
        "fmt"
)

func TestMocks(t *testing.T) {
        hd := master.GetTestDbConnection()
        master.ResetDb(hd)
        master.CreateTables(hd)
        fmt.Println("Test Basic...")
        if err := readWorkflow(hd, "test_files/test"); err != nil {
          log.Fatal(err)
        }
        fmt.Println("...passed")
        fmt.Println("Test Repeated Job...")
        if err := readWorkflow(hd, "test_files/test_repeated_job"); err != nil {
          log.Fatal(err)
        }
        fmt.Println("...passed")
        fmt.Println("Test Undefined Job...")
        if err := readWorkflow(hd, "test_files/test_undefined_job"); err != nil {
          log.Fatal(err)
        }
        fmt.Println("...passed")
}
