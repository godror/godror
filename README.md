[![Build Status](https://travis-ci.org/go-goracle/goracle.svg?branch=v2)](https://travis-ci.org/go-goracle/goracle)
[![GoDoc](https://godoc.org/gopkg.in/goracle.v2?status.svg)](http://godoc.org/gopkg.in/goracle.v2)

# goracle #
[goracle](driver.go) is a package which is a
[database/sql/driver.Driver](http://golang.org/pkg/database/sql/driver/#Driver)
for connecting to Oracle DB, using Anthony Tuininga's excellent OCI wrapper,
[ODPI-C](https://www.github.com/oracle/odpi).

## Rationale ##
With Go 1.9, driver-specific things are not needed, everything (I need) can be
achieved with the standard *database/sql* library. Even calling stored procedures
with OUT parameters, or sending/retrieving PL/SQL array types - just give a
`goracle.PlSQLArrays` Option within the parameters of `Exec`!

Connections are pooled by default (except `AS SYSOPER` or `AS SYSDBA`).

# Install #
It is `go get`'able  with `go get gopkg.in/goracle.v2`
iff you have
[ODPI-C](https://www.github.com/oracle/odpi) installed.

Otherwise, after the `go get` failed, [Install ODPI](https://oracle.github.io/odpi/doc/installation.html)

    cd $GOPATH/src/gopkg.in/goracle.v2
	go generate
	sudo cp -a odpi/lib/libodpic.so /usr/local/lib/
	sudo ldconfig /usr/local/lib
	cd ..

	go install

.
