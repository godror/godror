[![Build Status](https://travis-ci.org/go-goracle/goracle.svg?branch=v2)](https://travis-ci.org/go-goracle/goracle)
[![GoDoc](https://godoc.org/gopkg.in/goracle.v2?status.svg)](http://godoc.org/gopkg.in/goracle.v2)

# goracle #
[goracle](driver.go) is a package which is a
[database/sql/driver.Driver](http://golang.org/pkg/database/sql/driver/#Driver)
for connecting to Oracle DB, using Anthony Tuininga's excellent OCI wrapper,
[ODPI-C](https://www.github.com/oracle/odpi).

At least Go 1.9 is required!

## Rationale ##
With Go 1.9, driver-specific things are not needed, everything (I need) can be
achieved with the standard *database/sql* library. Even calling stored procedures
with OUT parameters, or sending/retrieving PL/SQL array types - just give a
`goracle.PlSQLArrays` Option within the parameters of `Exec`!

Connections are pooled by default (except `AS SYSOPER` or `AS SYSDBA`).

## Speed ##
Correctness and simplicity is more important than speed, but the underlying ODPI-C library
helps a lot with the lower levels, so the performance is not bad.

Queries are prefetched (128 rows), but you can speed up INSERT/UPDATE/DELETE statements
by providing all the subsequent parameters at once, by putting each param's subsequent
elements in a separate slice:

Instead of

    db.Exec("INSERT INTO table (a, b) VALUES (:1, :2)", 1, "a")
    db.Exec("INSERT INTO table (a, b) VALUES (:1, :2)", 2, "b")

do

    db.Exec("INSERT INTO table (a, b) VALUES (:1, :2)", []int{1, 2}, []string{"a", "b"})

.

## Logging ##
Goracle uses `github.com/go-kit/kit/log`'s concept of a `Log` function.
Either set `goracle.Log` to a logging function globally,
or (better) set the logger in the Context of ExecContext or QueryContext:

    db.QueryContext(context.WithValue(ctx, goracle.LogCtxKey, logger.Log), qry)

# Install #
Just

	go get gopkg.in/goracle.v2

and you're ready to go!

## Contribute ##
Just as with other Go projects, you don't want to change the import paths, but you can hack on the library
in place, just set up different remotes:

	cd $GOPATH.src/gopkg.in/goracle.v2
	git remote add upstream https://github.com/go-goracle/goracle.git
	git fetch upstream
	git checkout -b master upstream/master

	git checkout -f master
	git pull upstream master
	git remote add fork git@github.com:mygithubacc/goracle
	git checkout -b newfeature upstream/master

Change, experiment as you wish, then

	git commit -m 'my great changes' *.go
	git push fork newfeature

and you're ready to send a GitHub Pull Request from `github.com/mygithubacc/goracle`, `newfeature` branch.

As the ODPI-C sources are included as git submodule, don't forget to

	git submodule update --init
	# or
	go generate

to update ODPI-C, too.
If you want to refresh ODPI-C, you can:

	cd odpi
	git pull
	cd ..
	git add odpi
	git commit -m 'upgrade odpi to <git commit hash of odpi>' odpi

# Third-party #

  * [oracall](https://github.com/tgulacsi/oracall) generates a server for calling stored procedures.
  The "goracall" branch uses this library as the underying driver.

