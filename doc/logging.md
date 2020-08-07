[Contents](./contents.md)

# Go DRiver for ORacle User Guide

## Godror Logging and Tracing

### <a name="logging"></a> Logging

godror uses `github.com/go-kit/kit/log`'s concept of a `Log` function.  Either
set `godror.Log` to a logging function globally, or (better) set the logger in
the Context of `ExecContext` or `QueryContext`:

```go
db.QueryContext(godror.ContextWithLog(ctx, logger.Log), qry)
```

### <a name="tracing"></a> Tracing

Connection properties can be set for [end-to-end
tracing](https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-246A5A52-E666-4DBC-BDF6-98B83260A7AD).
Applications should set the properties because they can greatly help to identify
and resolve unnecessary database resource usage, or improper access.

`ClientIdentifier`, `ClientInfo`, `Module`, `Action` and `DbOp` can be set on
the session.  The values can then be seen by the DBA in databaase views such as
V$SESSION.  For example to set `Module` and `Action` on the Context:

```go
db.QueryContext(godror.ContextWithTraceTag(godror.TraceTag{
    Module: "processing",
    Action: "first",
}), qry)
```

### <a name="internaltracing"></a> Internal Tracing

The [ODPI-C tracing
capability](https://oracle.github.io/odpi/doc/user_guide/debugging.html) can be
used to log lower level database access.  For example to log executed statements
to the standard error stream set the environment variable `DPI_DEBUG_LEVEL` to
16 before running your application.  At a Windows command prompt, this could be
done with `set DPI_DEBUG_LEVEL=16`.  On Linux, you might use:

```
$ export DPI_DEBUG_LEVEL=16
$ node myapp.js 2> log.txt
```
