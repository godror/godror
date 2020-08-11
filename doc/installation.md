[Contents](./contents.md)

# Go DRiver for ORacle User Guide

## <a name="installation"></a> Godror Installation

Just run:

```bash
go get github.com/godror/godror
```

Then install Oracle Client libraries (see below) and you're ready to go!

Note that Windows may need some newer gcc (mingw-w64 with gcc 7.2.0).

### <a name="oracleclient"></a> Oracle Client Libraries

Although Oracle Client libraries are NOT required for compiling, they *are*
needed at run time.

To install and configure the Oracle client libraries, follow the general [ODPI-C
installation instructions](https://oracle.github.io/odpi/doc/installation.html).
Where those instructions mention the optional use of `oracleClientLibDir`,
instead in godror (on Windows and macOS) you can use the optional `libDir`
parameter to indicate the Oracle Client library directory:

```
db, err := sql.Open("godror", `user="scott" password="tiger" connectString="dbhost:1521/orclpdb1"
                               libDir="/Users/myname/instantclient_19_3"`)
```
