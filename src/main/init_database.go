package main

import (
	"client"
	"master"
)

/* Reset and initalize the master database tables. Note that this
 * will delete any existing data.
 *
 * Usage:
 *   go run init_dabatase.go
 *
 */
func main() {
	hd := master.GetDbConnection()
	master.ResetDb(hd)
	master.CreateTables(hd)
	client.Debug("Success")
}
