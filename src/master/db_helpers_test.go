package master

import (
	"testing"
)

func TestGetDbConnection(t *testing.T) {
	GetDbConnection()
}

func TestCreateTables(t *testing.T) {
	hd := GetTestDbConnection()
	ResetDb(hd)
	CreateTables(hd)
}
