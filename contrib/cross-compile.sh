#!/bin/sh
if [ $# -lt 2 ]; then
	echo "Usage: $0 <target-os> <target-arch> [go build|install]" >&2
	echo "  For example: $0 windows 386 go install" >&2
	exit 1
fi

dir="$(cd "$(dirname "$0")" || exit $?; pwd)"
zig=$(cd "$dir" && go run ./getzig.go)
arch="$2"
case "$arch" in
	amd64) arch=x86_64 ;;
    ppc64) arch=powerpc64 ;;
	386) arch=i386 ;;
esac
target="${arch}-${1}-gnu"
export CGO_ENABLED=1 
export GOOS="$1" 
export GOARCH="$2" 
export CC="${zig}cc -target $target" 
export CXX="${zig}xx -target $target"
shift 2
echo "env 'CGO_ENABLED=$CGO_ENABLED' 'GOOS=$GOOS' 'GOARCH=$GOARCH' 'CC=$CC' 'CXX=$CXX'"
if [ $# -gt 0 ]; then
	set -x
	exec env "CGO_ENABLED=$CGO_ENABLED" "GOOS=$GOOS" "GOARCH=$GOARCH" "CC=$CC" "CXX=$CXX" "$@"
fi
