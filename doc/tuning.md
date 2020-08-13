[Contents](./contents.md)

# Go DRiver for ORacle User Guide

## <a name="tuning"></a> Godror Tuning

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

### <a name="roundtrips"></a> Round-trips between Godror and Oracle Database

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

### <a name="queryperformance"></a> Query Performance

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
    rows, err := db.Query(sql, godror.PrefetchCount(1000), godror.FetchArraySize(1000))
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
        godror.PrefetchCount(mymaxnumrows+1), godror.FetchArraySize(mymaxnumrows))
    ```

    This will return all rows for the query in one round-trip.

- If you know that a query returns just one row then set
  `FetchArraySize()` to 1 to minimize memory usage.  The default
  prefetch value of 2 allows minimal round-trips for single-row
  queries:

    ```go
    sql := "SELECT last_nmae FROM employees WHERE employee_id = 100"
    err := db.QueryRow(sql, godror.FetchArraySize(1))
    ```

### <a name="dmlperformance"></a> DML Performance

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
