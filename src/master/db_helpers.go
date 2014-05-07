package master

import (
	"github.com/eaigner/hood"
	"os"
)

/* Create a db connection based on a configuration string */
func GetDbConnectionByConfig(config string) *hood.Hood {
	hd, err := hood.Open("postgres", config)
	if err != nil {
		panic(err)
	}
	return hd
}

/* Get the production DB connection based on the environment variable */
func GetDbConnection() *hood.Hood {
	config := os.Getenv("PQ_CONN")
	return GetDbConnectionByConfig(config)
}

/* Get the testing DB connection based on the environment variable */
func GetTestDbConnection() *hood.Hood {
	config := os.Getenv("PQ_CONN_TEST")
	return GetDbConnectionByConfig(config)
}

/* Delete all tables and indexes in the database */
func ResetDb(hd *hood.Hood) {
	_, err := hd.Db.Query("drop schema public cascade; create schema public;")
	if err != nil {
		panic(err)
	}
}

/* Create all of the tables in the master schema */
func CreateTables(hd *hood.Hood) {
	tx := hd.Begin()

	// List all table names here
	tables := []interface{}{
		&Rdd{}, &Segment{}, &RddEdge{}, &Workflow{}, &WorkflowEdge{},
		&Protojob{}, &WorkflowBatch{}, &Worker{}, &SegmentCopy{},
	}

	for _, table := range tables {
		err := tx.CreateTable(table)
		if err != nil {
			panic(err)
		}
	}

	err := tx.Commit()
	if err != nil {
		panic(err)
	}
}
