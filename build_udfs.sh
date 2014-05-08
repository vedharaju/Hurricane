#!/bin/bash

IFS=$'\n'

for f in `find $GOPATH/src/demo -iname *.go`
do
  echo building ${f%.*}.udf
  go build -o ${f%.*}.udf $f
done
