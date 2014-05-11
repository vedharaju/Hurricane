package main

import "worker"
import "os"
import "os/signal"
import "client"

func printUsage() {
	client.Debug("Usage\n  go run start_worker.go worker_interface:port master_interface:port\n")
	client.Debug("Example ports\n  localhost:1324\n  :2112\n  192.168.0.15:3333")
}

func waitForInterrupt() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	for sig := range c {
		client.Debug("\ncaptured signal, stopping and exiting.\n", sig)
		return
	}
}

func main() {
	if len(os.Args) != 3 {
		printUsage()
		return
	}

	workerhost := os.Args[1]
	masterhost := os.Args[2]
	client.Debug("Starting server on", workerhost)
	client.Debug("Press Ctrl-C to stop")
	worker.StartServer(workerhost, masterhost)

	waitForInterrupt()
}
