[Contents](./contents.md)

# Go DRiver for ORacle User Guide

## <a name="connection"></a> Godror Connection Handling

Connect to Oracle Database using `sql.Open("godror", dataSourceName)` where
`dataSourceName` contains options such as the user credentials, the database
connection string, and other configuration settings.
It should be a [logfmt](https://brandur.org/logfmt)-encoded  parameter list.
For example:

```
db, err := sql.Open("godror", `user="scott" password="tiger" connectString="dbhost:1521/orclpdb1"
    poolSessionTimeout=42s configDir="/tmp/admin"
    heterogeneousPool=false standaloneConnection=false`)
```
Other connection and driver options can also be used:
```
db, err := sql.Open("godror", `user="scott" password="tiger"
    connectString="dbhost:1521/orclpdb1?connect_timeout=2"
    poolSessionTimeout=42s configDir="/opt/oracle/configdir"
    heterogeneousPool=false standaloneConnection=false`)
```

All [godror
parameters](https://pkg.go.dev/github.com/godror/godror?tab=doc#pkg-overview)
should also be logfmt-ted.

You can provide all possible options with `ConnectionParams`:

    var P godror.ConnectionParams
    P.Username, P.Password = "scott", godror.NewPassword("tiger")
    P.ConnectString = "dbhost:1521/orclpdb1?connect_timeout=2"
    P.SessionTimeout = 42 * time.Second
    P.SetSessionParamOnInit("NLS_NUMERIC_CHARACTERS", ",.")
    P.SetSessionParamOnInit("NLS_LANGUAGE", "FRENCH")
    fmt.Println(P.StringWithPassword())
    db := sql.OpenDB(godror.NewConnector(P))

Or if you really want to build it "by hand", use `connstr.AppendLogfmt`:

    var buf strings.Builder
    connstr.AppendLogfmt(&buf, "user", "scott")
    connstr.AppendLogfmt(&buf, "password", "tiger")
    connstr.AppendLogfmt(&buf, "connectString", "dbhost:1521/orclpdb1?connect_timeout=2")
    fmt.Println(buf.String())

Note `ConnectionParams.String()` *redacts* the password (for security, to avoid
logging it - see <https://github.com/go-goracle/goracle/issues/79>).  If you
need the password, then use `ConnectionParams.StringWithPassword()`.

### <a name="connectionsstrings"></a> Connection Strings

The `sql.Open()` data source name `connectString` parameter or
`ConnectionParams` field `ConnectString` can be one of:

- An Easy Connect String

   [Easy Connect strings](https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-B0437826-43C1-49EC-A94D-B650B6A4A6EE)
   like the one shown above are most convenient to use.   The syntax allows a
   number of options to be set without requiring an external `tnsnames.ora`
   configuration file.   A more complex example usable with Oracle Client
   libraries 19c or later is:

   ```
   connectString="tcps://salesserver1:1521/sales.us.example.com?ssl_server_cert_dn=\"cn=sales,cn=Oracle Context Server,dc=us,dc=example,dc=com\"&sdu=8128&connect_timeout=60"
   ```

   Common options are `connect_timeout` to return an error if connection takes
   too long, and `expire_time` to make sure idle connections are not closed by
   firewalls.

   The technical article [Oracle Database 19c Easy Connect Plus Configurable
   Database Connection Syntax](https://download.oracle.com/ocomdocs/global/Oracle-Net-19c-Easy-Connect-Plus.pdf)
   contains more information.

- An Oracle Net Connect Descriptor String

   Full descriptors can be used, such as
   `connectString="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=dbhost.example.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=orclpdb1)))"`

- An Oracle Net Service Name

   Connect Descriptor Strings are commonly stored in a `tnsnames.ora` file and
   associated with a Net Service Name.   This name can be used directly for
   `connectString`.   For example, given a `tnsnames.ora` file with the following
   contents:

         ORCLPDB1 =
            (DESCRIPTION =
               (ADDRESS = (PROTOCOL = TCP)(HOST = dbhost.example.com)(PORT = 1521))
               (CONNECT_DATA =
                  (SERVER = DEDICATED)
                  (SERVICE_NAME = orclpdb1)
               )
            )

   then you could connect using `connectString=ORCLPDB1`

   See "Optional Oracle Net Configuration Files" below.

### <a name="clientconfigfiles"></a> Optional Oracle Net Configuration Files

Optional Oracle Net configuration files are used by the Oracle Client libraries
during the first call to `sql.Open`.   The directory containing the files can be
specified in the `sql.Open()` data source name with the `configDir` option.

The common files are:

* `tnsnames.ora`: A configuration file that defines databases aliases for
   establishing connections.

* `sqlnet.ora`: A profile configuration file that may contain information on
   features such as connection failover, network encryption, logging, and
   tracing.   See [Oracle Net Services
   Reference](https://www.oracle.com/pls/topic/lookup?ctx=dblatest&id=GUID-19423B71-3F6C-430F-84CC-18145CC2A818)
   for more information.

The files should be in a directory accessible to godror, not on the database
server host.

If the `configDir` connection option is not used, then a search heuristic is
used to find the directory containing the files.
The search includes:

* `$TNS_ADMIN`
* `/opt/oracle/instantclient_19_8/network/admin` if Instant Client is in `/opt/oracle/instantclient_19_8`.
* `/usr/lib/oracle/19.8/client64/lib/network/admin` if Oracle 19.8 Instant Client RPMs are used on Linux.
* `$ORACLE_HOME/network/admin` if godror is using libraries from a database installation.

### <a name="pooling"></a> Oracle Session Pooling

Set `standaloneConnection=0` - this is the default.  The old advice of setting
`db.SetMaxIdleConns(0)` are obsolete with Go 1.14.6.  It does no harm, but the
revised connection pooling (synchronous ResetSession before pooling the
connection) eliminates the need for it.

To use heterogeneous pools, set `heterogeneousPool=1` and provide the username
and password through `godror.ContextWithUserPassw` or `godror.ContextWithParams`.

***WARNING*** if you cannot use Go 1.14.6 or newer, then either set `standaloneConnection=1` or
disable Go connection pooling by `db.SetMaxIdleConns(0)` - they do not work well together, resulting in stalls!

### Backward compatibility

For backward compatibility, you can still provide _ANYTHING_ as the dataSourceName,
if it is one line, and is not logfmt-encoded, then it will be treated as a connectString.
So

  * `scott@tcps://salesserver1:1521/sales.us.example.com?ssl_server_cert_dn="cn=sales,cn=Oracle Context Server,dc=us,dc=example,dc=com"&sdu=8128&connect_timeout=60`
  * `scott/tiger@salesserver1/sales.us.example.com`
  * `oracle://scott:tiger@salesserver1/sales.us.example.com&poolSessionTimeout=42s`
  * `scott@tcps://salesserver1:1521/sales.us.example.com?ssl_server_cert_dn="cn=sales,cn=Oracle Context Server,dc=us,dc=example,dc=com"&sdu=8128&connect_timeout=60`

works, too.
