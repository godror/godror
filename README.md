![Go](https://github.com/godror/godror/workflows/Go/badge.svg)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/godror/godror)](https://pkg.go.dev/github.com/godror/godror)
[![Go Report Card](https://goreportcard.com/badge/github.com/godror/godror)](https://goreportcard.com/report/github.com/godror/godror)
[![codecov](https://codecov.io/gh/godror/godror/branch/master/graph/badge.svg)](https://codecov.io/gh/godror/godror)

# Go DRiver for ORacle

[godror](https://godoc.org/pkg/github.com/godror/godror) is a package which is a
[database/sql/driver.Driver](http://golang.org/pkg/database/sql/driver/#Driver)
for connecting to Oracle DB, using Anthony Tuininga's excellent OCI wrapper,
[ODPI-C](https://www.github.com/oracle/odpi).

At least Go 1.11 is required!
Cgo is required, so cross-compilation is hard, and you cannot set `CGO_ENABLED=0`!

Although an Oracle client is NOT required for compiling, it *is* at run time.
One can download it from <https://www.oracle.com/database/technologies/instant-client/downloads.html>

## Connect

In `sql.Open("godror", connString)`, where `connString` is a [logfmt](https://brandur.org/logfmt)-encoded 
parameter list, where you specify at least "username", "password" and "dsn" [1].
The "dsn" can be can be _ANYTHING_ that sqlplus or OCI accepts: a service name, a `host:port/service_name`, 
a `(DESCRIPTION=...)`, or an [Easy Connect Naming](https://download.oracle.com/ocomdocs/global/Oracle-Net-19c-Easy-Connect-Plus.pdf).

All godror params ([see](https://pkg.go.dev/github.com/godror/godror?tab=doc#pkg-overview)) should also be specified logfmt-ted. 

So 

```
username=scott password=tiger \
dsn="tcps://salesserver1:1521/sales.us.example.com?ssl_server_cert_dn=\"cn=sales,cn=Oracle Context Server,dc=us,dc=example,dc=com\"&sdu=8128&connect_timeout=60" \
poolSessionTimeout=42s libDir=/tmp/admin heterogeneousPool=false standaloneConnection=false
```

will connect to "salesserver1", port 1521, as scott/tiger, using the Easy Connect Naming syntax.

You can provide all possible options with `ConnectionParams`.
Watch out the `ConnectionParams.String()` does *redact* the password
(for security, to avoid logging it - see <https://github.com/go-goracle/goracle/issues/79>).
So use `ConnectionParams.StringWithPassword()`.

More advanced configurations can be set with a connection string such as:
`username=user password=pass dsn="(DESCRIPTION=(CONNECT_TIMEOUT=3)(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=hostname)(PORT=port)))(CONNECT_DATA=(SERVICE_NAME=sn)))"`

A configuration like this is how you would add functionality such as load balancing across multiple servers. 
The portion described in parenthesis above can also be set in the `DSN` field of `ConnectionParams`.

For other possible connection strings, see 
[node-oracledb connection strings](https://oracle.github.io/node-oracledb/doc/api.html#connectionstrings)
and [Easy Connect Naming](https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-B0437826-43C1-49EC-A94D-B650B6A4A6EE)
and [Oracle Database 19c Easy Connect Plus Configurable Database Connection](https://download.oracle.com/ocomdocs/global/Oracle-Net-19c-Easy-Connect-Plus.pdf).

TL;DR; the short form is `username=username password=password dsn="[//]host[:port][/service_name][:server][/instance_name]"`, the long form is
`dsn="(DESCRIPTION= (ADDRESS=(PROTOCOL=tcp)(HOST=host)(PORT=port)) (CONNECT_DATA= (SERVICE_NAME=service_name) (SERVER=server) (INSTANCE_NAME=instance_name)))"`.

The names may be set in `tnsnames.ora` and other params set in `sqlnet.ora`.
It's been searched at `TNS_ADMIN` environment variable, which can be set before the first call to
`sql.Open`, or set as the `libDir` connection parameter.

To use heterogeneous pools, set `heterogeneousPool=1` and provide the username/password through
`godror.ContextWithUserPassw` or `godror.ContextWithParams`.

### Oracle Session Pooling
Set `standaloneConnection=0`- this is the default. 
All old advices of `db.SetMaxIdleConns(0)` are obsolete with Go 1.14.6.
It does no harm, but the revised connection pooling (synchronous ResetSession before pooling the connection)
eliminates the need for it.

***WARNING*** if you cannot use Go 1.14.6 or newer, then either set `standaloneConnection=1` or
disable Go connection pooling by `db.SetMaxIdleConns(0)` - they do not work well together, resulting in stalls!

## Rationale

With Go 1.9, driver-specific things are not needed, everything (I need) can be
achieved with the standard _database/sql_ library. Even calling stored procedures
with OUT parameters, or sending/retrieving PL/SQL array types - just give a
`godror.PlSQLArrays` Option within the parameters of `Exec`!

The array size of the returned PL/SQL arrays can be set with `godror.ArraySize(2000)` (default value is 1024).

## Speed

Correctness and simplicity is more important than speed, but the underlying ODPI-C library
helps a lot with the lower levels, so the performance is not bad.

Queries are prefetched (256 rows by default, can be changed by adding a
`godror.FetchRowCount(1000)` argument to the call of Query),
but you can speed up INSERT/UPDATE/DELETE statements
by providing all the subsequent parameters at once, by putting each param's subsequent
elements in a separate slice:

Instead of

```go
db.Exec("INSERT INTO table (a, b) VALUES (:1, :2)", 1, "a")
db.Exec("INSERT INTO table (a, b) VALUES (:1, :2)", 2, "b")
```

do

```go
db.Exec("INSERT INTO table (a, b) VALUES (:1, :2)", []int{1, 2}, []string{"a", "b"})
```

## Logging

godror uses `github.com/go-kit/kit/log`'s concept of a `Log` function.
Either set `godror.Log` to a logging function globally,
or (better) set the logger in the Context of ExecContext or QueryContext:

```go
db.QueryContext(godror.ContextWithLog(ctx, logger.Log), qry)
```

## Tracing

To set ClientIdentifier, ClientInfo, Module, Action and DbOp on the session,
to be seen in the Database by the Admin, set godror.TraceTag on the Context:

```go
db.QueryContext(godror.ContextWithTraceTag(godror.TraceTag{
    Module: "processing",
    Action: "first",
}), qry)
```

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
[presentation about Go](https://static.rainfocus.com/oracle/oow18/sess/1525791357522001Q5tc/PF/DEV5047%20-%20The%20Go%20Language_1540587475596001afdk.pdf)
(page 39)!

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

# Install

Just

```bash
go get github.com/godror/godror
```

and you're ready to go!

Note that Windows may need some newer gcc (mingw-w64 with gcc 7.2.0).

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

Change, experiment as you wish, then

```bash
git commit -m 'my great changes' *.go
git push fork newfeature
```

and you're ready to send a GitHub Pull Request from `github.com/mygithubacc/godror`, `newfeature` branch.

### pre-commit

Add this to .git/hooks/pre-commit (after downloaded a [staticcheck](https://staticcheck.io) [release](https://github.com/dominikh/go-tools/releases)):

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


---
[1] For backward compatibility, you can provide _ANYTHING_ on the first line, and logfmt-ed parameters
on the second line. Or no logfmt-ed parameters at all.
So `scott@tcps://salesserver1:1521/sales.us.example.com?ssl_server_cert_dn="cn=sales,cn=Oracle Context Server,dc=us,dc=example,dc=com"&sdu=8128&connect_timeout=60
poolSessionTimeout=42s password=tiger
` works, too.
