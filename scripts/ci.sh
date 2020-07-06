#!/bin/bash

#
# Run the CI flow and build the binary
# Prerequisite -
# 1. Go runtime
#

# absolute directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

ALL_PKGS=""

cd $DIR/../src
# test lint, vet, and build as basic build steps in CI
echo run golint
golint ./...
echo run go vet
go vet ./...

echo run go build
mkdir -p ${DIR}/../bin
rm -f ${DIR}/../bin/pulsar-monitor
go build -o ${DIR}/../bin/pulsar-monitor .

# Memory heap and stack allocation analysis for GC optimization
# go build -gcflags -m -o ${DIR}/../bin/pulsar-monitor .
