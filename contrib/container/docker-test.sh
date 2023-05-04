#!/usr/bin/env bash
set -eu
echo "\$@=$@"
dir="$(cd "$(dirname "$0")"; pwd)/../.."
DOCKER="${DOCKER:-$(command -v podman 2>/dev/null || echo docker)}"
mkdir -p "${dir}/testdata/cache/go-"{mod,build}
u=$(id -u)
un=$(id -un)
g=$(id -g)
gn=$(id -gn)
set -x
#go test -c -o "${dir}/testdata/godror.test"
${DOCKER} run -t -i --rm --name godror.test.build \
    -v "${dir}:/app:rw" \
    -v "${dir}/testdata/cache/go-mod:/go/pkg/mod:rw" \
    -v "${dir}/testdata/cache/go-build:/root/.cache/go-build:rw" \
    -w /app \
    docker.io/library/golang:1.20.4-bullseye \
    sh -c "go test -c -o /app/testdata/godror.test && chown '$u:$g' /app/testdata/godror.test"
ls -l "${dir}/testdata/"

exec ${DOCKER} run -t -i --rm \
    --name 21client \
    -v "${dir}:/app" \
    -w /app \
    ghcr.io/oracle/oraclelinux8-instantclient:21 \
    sh -c "groupadd -g $g $gn; useradd -u $u -g $g -G users -s /bin/sh '$un'; chown '$u:$g' -R /app; stat testdata/godror.test; su '$un' -c 'id; set -x; /app/testdata/godror.test \"$@\"'"

