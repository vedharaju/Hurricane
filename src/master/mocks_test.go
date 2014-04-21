package master

import (
	"testing"
)

func TestMocks(t *testing.T) {
	hd := GetTestDbConnection()
	ResetDb(hd)
	CreateTables(hd)
	MockDiamondWorkflow(hd)
}
