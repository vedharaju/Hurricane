package master

import (
  "testing"
  "fmt"
)

func TestMocks(t *testing.T) {
  hd := GetTestDbConnection()
  ResetDb(hd)
  CreateTables(hd)
  MockDiamondWorkflow(hd)
}
