package main

import (
  "strconv"
  "os"
)

func main() {
  fo, err := os.Create("log.txt")
  if err != nil { panic(err) }

  defer func() {
    if err := fo.Close(); err != nil {
      panic(err)
    }
  }()


  for i := 0; i <= 10; i++ {
    for j := 0; j <= 1000000; j++ {
      timestamp := strconv.Itoa(i) + ":" + strconv.Itoa(j)
      if j % 100 == 0 {
        fo.WriteString(timestamp + "," + "error\n")
      } else {
        fo.WriteString(timestamp + "," + "pass\n")
      }
    }
  }
}
