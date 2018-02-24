# ODPI-C version 2.2.0

Oracle Database Programming Interface for C (ODPI-C) is an open source library
of C code that simplifies access to Oracle Database for applications written in
C or C++.  It is a wrapper over [Oracle Call Interface
(OCI)](http://www.oracle.com/technetwork/database/features/oci/index.html) that
makes applications and language interfaces easier to develop.

## Features

- 11.2, 12.1 and 12.2 Oracle Client support
- 9.2 and higher Oracle Database support (depending on Oracle Client version)
- SQL and PL/SQL execution
- Character datatypes (CHAR, VARCHAR2, NCHAR, NVARCHAR2, CLOB, NCLOB, LONG)
- Numeric datatypes (NUMBER, BINARY_FLOAT, BINARY_DOUBLE)
- Dates, Timestamps, Intervals
- Binary types (BLOB, BFILE, RAW, LONG RAW)
- PL/SQL datatypes (PLS_INTEGER, BOOLEAN, Collections, Records)
- JSON
- User Defined Types
- REF CURSOR, Nested cursors, Implicit Result Sets
- Array fetch
- Array bind/execute
- Batch errors
- Array DML Row Counts
- Scrollable cursors
- DML RETURNING
- Query Result Caching
- Statement caching (with tagging)
- Query metadata
- Standalone connections
- Pooled connections via OCI session pools (Homogeneous and non-homogeneous with proxy authentication)
- Database Resident Connection Pooling (DRCP)
- External authentication
- Proxy authentication
- Privileged connection support (SYSDBA, SYSOPER, SYSASM, PRELIM_AUTH)
- Session tagging
- Connection validation (when acquired from session pool or DRCP)
- Password change
- End-to-end tracing, mid-tier authentication and auditing (action, module,
  client identifier, client info, database operation)
- Edition Based Redefinition
- Database startup/shutdown
- Application Continuity
- Two Phase Commit
- Continuous Query Notification
- Advanced Queuing
- Sharded Databases
- Easily extensible via direct OCI calls

## Install

See
[ODPI-C Installation](https://oracle.github.io/odpi/doc/installation.html)
for detailed instructions.

A sample Makefile (Makefile.win32 for use with nmake on Windows) is provided if
you wish to build ODPI-C as a shared library. Otherwise, include the ODPI-C
source code in your project (see embed/dpi.c).

Oracle Client libraries are required.  They are available in the free [Oracle
Instant
Client](http://www.oracle.com/technetwork/database/database-technologies/instant-client/overview/index.html)
"Basic" and "Basic Light" packages, or available in any Oracle Database
installation or full Oracle Client installation.

ODPI-C uses the shared library loading mechanism available on each supported
platform to load the Oracle Client libraries at runtime. This allows code using
ODPI-C to be built only once, and then run using Oracle Client 11.2, 12.1 or
12.2 libraries. Oracle's standard client-server version interoperability allows
connection to both older and newer databases, for example Oracle 12.2 client
libraries can connect to Oracle Database 11.2 or later.


ODPI-C has been tested on Linux, Windows and macOS.  Other platforms should
also work but have not been tested.  On Windows, Visual Studio 2008 or higher
is required.  On macOS, Xcode 6 or higher is required.  On Linux, GCC 4.4 or
higher is required.

## Documentation

See [ODPI-C Documentation](https://oracle.github.io/odpi/doc/index.html) and
[Release Notes](https://oracle.github.io/odpi/doc/releasenotes.html).

## Help

Please report bugs and ask questions using [GitHub issues](https://github.com/oracle/odpi/issues).

## Samples

See [/samples](https://github.com/oracle/odpi/tree/master/samples)

## Tests

See [/test](https://github.com/oracle/odpi/tree/master/test)

## Contributing

See [CONTRIBUTING](https://github.com/oracle/odpi/blob/master/CONTRIBUTING.md)

## Drivers Using ODPI-C

Drivers based on ODPI-C that are maintained by Oracle:
* [cx_Oracle](https://oracle.github.io/python-cx_Oracle) driver for Python.
* [node-oracledb](https://github.com/oracle/node-oracledb/tree/dev-2.0) for
  Node.js.

Other communities are also using ODPI-C for Rust and Go drivers.

## License

Copyright (c) 2016, 2017 Oracle and/or its affiliates.  All rights reserved.

This program is free software: you can modify it and/or redistribute it under
the terms of:

(i)  the Universal Permissive License v 1.0 or at your option, any
     later version (<http://oss.oracle.com/licenses/upl>); and/or

(ii) the Apache License v 2.0. (<http://www.apache.org/licenses/LICENSE-2.0>)
