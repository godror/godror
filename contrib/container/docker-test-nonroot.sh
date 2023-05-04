#!/bin/sh
set -eu
dir="$(cd "$(dirname "$0")"; pwd)/../.."
DOCKER="${DOCKER:-$(command -v podman 2>/dev/null || echo docker)}"

tar cf - . | shasum >/tmp/sha.sum
${DOCKER} build -t godror/test -v "${dir}:/app" "${dir}/contrib/container/test/"
${DOCKER} run -t -i --rm --name godror.test -u test -v "${dir}:/app" godror/test "$@"
