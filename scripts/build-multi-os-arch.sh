#!/bin/bash

set -e

#
# build releases for multiple OS and Architecture
#

# absolute directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

VERSION="$( cat ../Makefile  | grep "^TAG" | cut -d' ' -f3 )"

function build_os_arch() {
  OS=$1
  ARCH=$2
  echo "build for OS $1 ARCH $2"
  GOOS=$OS GOARCH=$ARCH go build -o ${DIR}/../bin/releases/pulsar-heartbeat-$VERSION-$OS-$ARCH .
  gzip ${DIR}/../bin/releases/pulsar-heartbeat-$VERSION-$OS-$ARCH
}
cd $DIR/../src

echo "run go build for version ${VERSION}"
mkdir -p ${DIR}/../bin/releases
rm -f ${DIR}/../bin/releases/pulsar-heartbeat*

build_os_arch "darwin" "amd64"
build_os_arch "freebsd" "amd64"
build_os_arch "freebsd" "arm64"
build_os_arch "linux" "amd64"
build_os_arch "linux" "arm"
build_os_arch "linux" "arm64"
build_os_arch "linux" "ppc64"
build_os_arch "openbsd" "amd64"
build_os_arch "openbsd" "arm64"
build_os_arch "windows" "amd64"
build_os_arch "windows" "arm"

# Not supported
# build_os_arch "darwin" "arm64"
# build_os_arch "solaris" "amd64"
# build_os_arch "windows" "386"
