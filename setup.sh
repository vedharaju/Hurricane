#!/bin/bash

if [ ! -d "./src/github.com/eaigner/hood" ]; then
  mkdir -p ./src/github.com/eaigner/
  pushd ./src/github.com/eaigner
  git clone https://github.com/eaigner/hood.git
  popd
fi

if [ ! -d "./src/github.com/lib/pq" ]; then
  mkdir -p ./src/github.com/lib/
  pushd ./src/github.com/lib
  git clone https://github.com/lib/pq/
  popd
fi

echo \
  "Remember to set your environment variables. Sample" \
  "configuration can be found in sample_env."
