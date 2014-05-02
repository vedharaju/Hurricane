package main

import "os"
import "syscall"
import "bufio"
import "log"
import "worker"
import "path"

func main() {
	gopath := os.Getenv("GOPATH")
  path := path.Join(gopath,"/src/demo/wordcount/crime_and_punishment.txt")

  file, err := os.Open(path)
  if err != nil {
    log.Panic(err);
  }
  defer file.Close()

  stdout := os.NewFile(uintptr(syscall.Stdout), "/dev/stdout")

  scanner := bufio.NewScanner(file)
  for scanner.Scan() {
    tuple := worker.Tuple{[]string{scanner.Text()}}

    stdout.Write(tuple.SerializeTuple(0))
    stdout.Write([]byte{'\n'})
  }
}
