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

In `sql.Open("godror", dataSourceName)`, 
where `dataSourceName` is a [logfmt](https://brandur.org/logfmt)-encoded 
parameter list, where you specify at least "user", "password" and "connectString".
The "connectString" can be _ANYTHING_ that SQL*Plus or OCI accepts: 
a service name, an 
[Easy Connect string](https://download.oracle.com/ocomdocs/global/Oracle-Net-19c-Easy-Connect-Plus.pdf) 
like `host:port/service_name`, or a connect descriptor like `(DESCRIPTION=...)`.

All godror params ([see](https://pkg.go.dev/github.com/godror/godror?tab=doc#pkg-overview)) should also be specified logfmt-ted. 

For more connection details, see [dsn/README.md](./dsn/README.md).

You can provide all possible options with `ConnectionParams`.   Note
`ConnectionParams.String()` *redacts* the password (for security, to avoid
logging it - see <https://github.com/go-goracle/goracle/issues/79>).   If you
need the password, then use `ConnectionParams.StringWithPassword()`.

### Oracle Session Pooling
Set `standaloneConnection=0`- this is the default.   The old advice of setting
`db.SetMaxIdleConns(0)` are obsolete with Go 1.14.6.   It does no harm, but the
revised connection pooling (synchronous ResetSession before pooling the
connection) eliminates the need for it.

***WARNING*** if you cannot use Go 1.14.6 or newer, then either set `standaloneConnection=1` or
disable Go connection pooling by `db.SetMaxIdleConns(0)` - they do not work well together, resulting in stalls!

To use heterogeneous pools, set `heterogeneousPool=1` and provide the username
and password through `godror.ContextWithUserPassw` or
`godror.ContextWithParams`.
Set `standaloneConnection=0`- this is the default. 

## Rationale

With Go 1.9, driver-specific things are not needed, everything (I need) can be
achieved with the standard _database/sql_ library. Even calling stored procedures
with OUT parameters, or sending/retrieving PL/SQL array types - just give a
`godror.PlSQLArrays` Option within the parameters of `Exec`!

The array size of the returned PL/SQL arrays can be set with `godror.ArraySize(2000)` (default value is 1024).

## Tuning

Correctness and simplicity is more important than speed, but the underlying
ODPI-C library helps a lot with the lower levels, so the performance is not bad.

Some general recommendations:

* Tune your SQL statements.  See the [SQL Tuning
  Guide](https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=TGSQL).

  Use bind variables to avoid statement reparsing.

  Tune the `PrefetchCount()` and `FetchArraySize()` values for each query,

  Do simple optimizations like limiting the number of rows returned by queries,
  and avoiding selecting columns not used in the application.

  Make good use of PL/SQL to avoid executing many individual statements from
  godror.

  Tune the [statement
  cache](https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-4947CAE8-1F00-4897-BB2B-7F921E495175)
  size.  This is currently hardcoded as 40, but the value can be overridden in
  an [`oraaccess.xml`](https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-9D12F489-EC02-46BE-8CD4-5AECED0E2BA2) file.

  Enable [Client Result
  Caching](https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-35CB2592-7588-4C2D-9075-6F639F25425E)
  for small lookup tables.

* Tune your database.  See the [Database Performance Tuning Guide](https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=TGDBA).

* Tune your network.  For example, when inserting or retrieving a large number
  of rows (or for large data), or when using a slow network, then tune the
  Oracle Network Session Data Unit (SDU) and socket buffer sizes, see [Oracle
  Net Services: Best Practices for Database Performance and High
  Availability](https://static.rainfocus.com/oracle/oow19/sess/1553616880266001WLIh/PF/OOW19_Net_CON4641_1569022126580001esUl.pdf).

### Round-trips between Godror and Oracle Database

A round-trip is defined as the trip from the Oracle Client libraries (used by
godror) to the database and back.  Calling each godror function, or accessing
each attribute, will require zero or more round-trips.  Along with tuning an
application's architecture and [tuning its SQL
statements](https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=TGSQL), a
general performance and scalability goal is to minimize
[round-trips](https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-9B2F05F9-D841-4493-A42D-A7D89694A2D1).

Oracle's [Automatic Workload
Repository](https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-56AEF38E-9400-427B-A818-EDEC145F7ACD)
(AWR) reports show 'SQL*Net roundtrips to/from client' and are useful for
finding the overall behavior of a system.  Sometimes you may wish to find the
number of round-trips used for a specific application.  Snapshots of the
`V$SESSTAT` view taken before and after doing some work can be used for this:

```sql
SELECT ss.value, sn.display_name
FROM v$sesstat ss, v$statname sn
WHERE ss.sid = SYS_CONTEXT('USERENV','SID')
AND ss.statistic# = sn.statistic#
AND sn.name LIKE '%roundtrip%client%'
```

Note running this query will also affect the round-trip count.  You may want to
execute it in a second connection but replace the `SYS_CONTEXT()` call with the
value of first connection's SID.

### Query Performance

When queries (`SELECT` or `WITH` statements) are executed, the performance of
fetching the rows from the database can be tuned with `PrefetchCount()` and
`FetchArraySize()`.  The best values can be found by experimenting with your
application under the expected load of normal application use.  This is because
the cost of the extra memory copy from the prefetch buffers when fetching a
large quantity of rows or very "wide" rows may outweigh the cost of a round-trip
for a single godror user on a fast network.  However under production
application load, the reduction of round-trips may help performance and overall
system scalability.

Here are some suggestions for the starting point to begin your tuning:

- To tune queries that return an unknown number of rows, estimate the number of
  rows returned and start with an appropriate `FetchArraySize()` value.  The
  default is 100.  Then set `PrefetchCount()` to the `FetchArraySize()` value.
  Do not make the sizes unnecessarily large.  For example:

    ```go
    sql := "SELECT * FROM very_big_table"
    rows, err := db.Query(sql, godor.PrefetchCount(1000), godor.FetchArraySize(1000))
    ```

    Adjust the values as needed for performance, memory and round-trip
    usage.  For a large quantity of rows or very "wide" rows on fast
    networks you may prefer to leave `PrefetchCount()` at its default
    value of 2.  Keep `FetchArraySize()` equal to, or bigger than,
    `PrefetchCount()`.

- If you are fetching a fixed number of rows, start your tuning by setting
  `FetchArraySize()` to the number of expected rows, and set `PrefetchCount()`
  to one greater than this value.  (Adding one removes the need for a round-trip
  to check for end-of-fetch).  For example, if you are querying 20 rows then set
  `PrefetchCount()` to 21 and `FetchArraySize()` to 20:

    ```go
    myoffset := 0       // do not skip any rows (start at row 1)
    mymaxnumrows := 20  // get 20 rows

    sql := `SELECT last_name
            FROM employees
            ORDER BY last_name
            OFFSET :offset ROWS FETCH NEXT :maxnumrows ROWS ONLY`
    rows, err := db.Query(sql, myoffset, mymaxnumrows,
        godor.PrefetchCount(mymaxnumrows+1), godor.FetchArraySize(mymaxnumrows))
    ```

    This will return all rows for the query in one round-trip.

- If you know that a query returns just one row then set
  `FetchArraySize()` to 1 to minimize memory usage.  The default
  prefetch value of 2 allows minimal round-trips for single-row
  queries:

    ```go
    sql := "SELECT last_nmae FROM employees WHERE employee_id = 100"
    err := db.QueryRow(sql, godor.FetchArraySize(1))
    ```

### DML Performance

Instead of looping over [DML
statements](https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-2E008D4A-F6FD-4F34-9071-7E10419CA24D)
(e.g. INSERT, UPDATE and DELETE), performance can be greatly improved by
providing all the data at once using slices.

For example, instead of:

```go
db.Exec("INSERT INTO table (a, b) VALUES (:1, :2)", 1, "a")
db.Exec("INSERT INTO table (a, b) VALUES (:1, :2)", 2, "b")
```

do:

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
