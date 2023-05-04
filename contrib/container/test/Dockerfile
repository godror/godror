# syntax=docker/dockerfile:1

FROM docker.io/library/golang:1.20.4-bullseye AS builder
MAINTAINER Tamás Gulácsi <tamas@gulacsi.eu>

RUN groupadd -g 1000 test
RUN useradd -u 1000 -g 1000 -G users -s /bin/sh test
USER test

VOLUME /app
WORKDIR /app
RUN mkdir -p /app/testdata 
#ADD /tmp/sha.sum /dev/null
RUN env GOMODCACHE=/app/testdata/cache/go-mod GOCACHE=/app/testdata/cache/go-cache go test -c -o /tmp/godror.test 
RUN ls -l testdata


FROM ghcr.io/oracle/oraclelinux8-instantclient:21 

RUN groupadd -g 1000 test
RUN useradd -u 1000 -g 1000 -G users -s /bin/sh test
USER test

WORKDIR /app
COPY --from=builder /tmp/godror.test /tmp/

ENTRYPOINT ["/tmp/godror.test"]
CMD []

