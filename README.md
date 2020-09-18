![Go](https://github.com/godror/godror/workflows/Go/badge.svg)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/godror/godror)](https://pkg.go.dev/github.com/godror/godror)
[![Go Report Card](https://goreportcard.com/badge/github.com/godror/godror)](https://goreportcard.com/report/github.com/godror/godror)
[![codecov](https://codecov.io/gh/godror/godror/branch/master/graph/badge.svg)](https://codecov.io/gh/godror/godror)

# Go DRiver for ORacle

[godror](https://godoc.org/pkg/github.com/godror/godror) is a package which is a
[database/sql/driver.Driver](http://golang.org/pkg/database/sql/driver/#Driver)
for connecting to Oracle DB, using Anthony Tuininga's excellent OCI wrapper,
[ODPI-C](https://www.github.com/oracle/odpi).

At least Go 1.13 is required!
Cgo is required, so cross-compilation is hard, and you cannot set `CGO_ENABLED=0`!

Although Oracle Client libraries are NOT required for compiling, they *are*
needed at run time.  Download the free Basic or Basic Light package from
<https://www.oracle.com/database/technologies/instant-client/downloads.html>.

### Rationale

With Go 1.9, driver-specific things are not needed, everything (I need) can be
achieved with the standard _database/sql_ library. Even calling stored
procedures with OUT parameters, or sending/retrieving PL/SQL array types - just
give a `godror.PlSQLArrays` Option within the parameters of `Exec`!  For
example, the array size of the returned PL/SQL arrays can be set with
`godror.ArraySize(2000)` (default value is 1024).

## Documentation

See [Godror API Documentation](https://pkg.go.dev/github.com/godror/godror?tab=doc) and
the [Godror User Guide](https://godror.github.io/godror/doc/contents.html).

## Installation

Run:

```bash
go get github.com/godror/godror
```

Then install Oracle Client libraries and you're ready to go!

See [Godror
Installation](https://godror.github.io/godror/doc/installation.html) for more information.

## Connection

To connect to Oracle Database use `sql.Open("godror", dataSourceName)`,
where `dataSourceName` is a [logfmt](https://brandur.org/logfmt)-encoded
parameter list.  Specify at least "user", "password" and "connectString".
For example:

```
db, err := sql.Open("godror", `user="scott" password="tiger" connectString="dbhost:1521/orclpdb1"`)
```

The `connectString` can be _ANYTHING_ that SQL*Plus or Oracle Call Interface
(OCI) accepts: a service name, an [Easy Connect
string](https://download.oracle.com/ocomdocs/global/Oracle-Net-19c-Easy-Connect-Plus.pdf)
like `host:port/service_name`, or a connect descriptor like `(DESCRIPTION=...)`.

For more connection options, see [Godor Connection
Handling](https://godror.github.io/godror/doc/connection.html).

## Extras

To use the godror-specific functions, you'll need a `*godror.conn`.
That's what `godror.DriverConn` is for!
See [z_qrcn_test.go](./z_qrcn_test.go) for using that to reach
[NewSubscription](https://godoc.org/github.com/godror/godror#Subscription).

### Calling stored procedures

Use `ExecContext` and mark each OUT parameter with `sql.Out`.

### Using cursors returned by stored procedures

Use `ExecContext` and an `interface{}` or a `database/sql/driver.Rows` as the `sql.Out` destination,
then either use the `driver.Rows` interface,
or transform it into a regular `*sql.Rows` with `godror.WrapRows`,
or (since Go 1.12) just Scan into `*sql.Rows`.

For examples, see Anthony Tuininga's
[presentation about Go](https://static.rainfocus.com/oracle/oow19/sess/1567058525476001cK8G/PF/DEV6708-Using-the-Go-Language-for-Efficient-Oracle-Database-Applications_1568841171132001jI7d.pdf)
(page 41)!

## Caveats

### sql.NullString

`sql.NullString` is not supported: Oracle DB does not differentiate between
an empty string ("") and a NULL, so an

```go
sql.NullString{String:"", Valid:true} == sql.NullString{String:"", Valid:false}
```

and this would be more confusing than not supporting `sql.NullString` at all.

Just use plain old `string` !

### NUMBER

`NUMBER`s are transferred as `string` to Go under the hood.
This ensures that we don't lose any precision (Oracle's NUMBER has 38 decimal digits),
and `sql.Scan` will hide this and `Scan` into your `int64`, `float64` or `string`, as you wish.

For `PLS_INTEGER` and `BINARY_INTEGER` (PL/SQL data types) you can use `int32`.

### CLOB, BLOB

From 2.9.0, LOBs are returned as string/[]byte by default (before it needed the `ClobAsString()` option).
Now it's reversed, and the default is string, to get a Lob reader, give the `LobAsReader()` option.

If you return Lob as a reader, watch out with `sql.QueryRow`, `sql.QueryRowContext` !
They close the statement right after you `Scan` from the returned `*Row`, the returned `Lob` will be invalid, producing
`getSize: ORA-00000: DPI-1002: invalid dpiLob handle`.

So, use a separate `Stmt` or `sql.QueryContext`.

For writing a LOB, the LOB locator returned from the database is valid only till the `Stmt` is valid!
So `Prepare` the statement for the retrieval, then `Exec`, and only `Close` the stmt iff you've finished with your LOB!
For example, see [z_lob_test.go](./z_lob_test.go), `TestLOBAppend`.

### TIMESTAMP

As I couldn't make TIMESTAMP arrays work, all `time.Time` is bind as `DATE`, so fractional seconds
are lost.
A workaround is converting to string:

```go
time.Now().Format("2-Jan-06 3:04:05.000000 PM")
```

See [#121 under the old project](https://github.com/go-goracle/goracle/issues/121).


### Stored procedure returning cursor (result set)
```go
var rset1, rset2 driver.Rows

query := `BEGIN Package.StoredProcA(123, :1, :2); END;`

if _, err := db.ExecContext(ctx, query, sql.Out{Dest: &rset1}, sql.Out{Dest: &rset2}); err != nil {
	log.Printf("Error running %q: %+v", query, err)
	return
}
defer rset1.Close()
defer rset2.Close()

cols1 := rset1.(driver.RowsColumnTypeScanType).Columns()
dests1 := make([]driver.Value, len(cols1))
for {
	if err := rset1.Next(dests1); err != nil {
		if err == io.EOF {
			break
		}
		rset1.Close()
		return err
	}
	fmt.Println(dests1)
}

cols2 := rset1.(driver.RowsColumnTypeScanType).Columns()
dests2 := make([]driver.Value, len(cols2))
for {
	if err := rset2.Next(dests2); err != nil {
		if err == io.EOF {
			break
		}
		rset2.Close()
		return err
	}
	fmt.Println(dests2)
}
```


## Contribute

Just as with other Go projects, you don't want to change the import paths, but you can hack on the library
in place, just set up different remotes:

```bash
cd $GOPATH/src/github.com/godror/godror
git remote add upstream https://github.com/godror/godror.git
git fetch upstream
git checkout -b master upstream/master

git checkout -f master
git pull upstream master
git remote add fork git@github.com:mygithubacc/godror
git checkout -b newfeature upstream/master
```

Change, experiment as you wish.  Then run

```bash
git commit -m 'my great changes' *.go
git push fork newfeature
```

and you're ready to send a GitHub Pull Request from the
`github.com/mygithubacc/godror` branch called `newfeature`.

### Pre-commit

Download a [staticcheck](https://staticcheck.io)
[release](https://github.com/dominikh/go-tools/releases) and add this to
.git/hooks/pre-commit:

```bash
#!/bin/sh
set -e

output="$(gofmt -l "$@")"

if [ -n "$output" ]; then
    echo >&2 "Go files must be formatted with gofmt. Please run:"
    for f in $output; do
        echo >&2 "  gofmt -w $PWD/$f"
    done
    exit 1
fi

exec staticcheck
```

# Third-party

* [oracall](https://github.com/tgulacsi/oracall) generates a server for calling stored procedures.
