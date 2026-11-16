# Timezones
Oracle DB has three time zones: 

  - SESSIONTIMEZONE - can be set at session level, changes CURRENT_DATE, CURRENT_TIMESTAMP, LOCALTIMESTAMP
  - DBTIMEZONE - only effects TIMESTAMP WITH LOCAL TIME ZONE column
  - The database server's OS' time zone - effects SYSDATE, SYSTIMESTAMP

For details, see https://stackoverflow.com/a/29272926

Go does have a time zone information for every time.Time value.
So, how do we determine what time zone should be set on a value returned by a `SELECT DATE_column FROM table` ?

If you insert SYSDATE into that column, then the OS' time zone is the relevant.
If you insert CURRENT_DATE, then SESSIONTIMEZONE is the relevant.

Godror uses SESSIONTIMEZONE for DATE and TIMESTAMP (without time zone) values,
both for writing a `time.Time` and for reading one (see [#495](https://github.com/godror/godror/issues/495)).
This is what the server itself uses when it converts between DATE and
TIMESTAMP WITH (LOCAL) TIME ZONE, so a `time.Time` bound as DATE
(`BindTimeAsDate`, `PlSQLArrays`) is the same instant in every column type.
(Earlier the DB's OS' time zone, `TO_CHAR(SYSTIMESTAMP, 'TZR')`, was used.)

The session time zone is read once, after the session has been initialized
(`onInit`, `alterSession`):

  - a region name (`ORA_SDTZ=Europe/Berlin`, `ALTER SESSION SET TIME_ZONE='Europe/Berlin'`)
    is used as is, with daylight saving;
  - an offset (`+01:00`) is a fixed zone - except when it is the current offset
    of the process' local time zone, then `time.Local` is used.

Without `ORA_SDTZ` or `ALTER SESSION` the session time zone is the *current offset*
of the client's OS time zone, and the server does not know about daylight saving
in that session: godror then uses `time.Local`, but the server's own conversions
of dates of the other half of the year (a `BindTimeAsDate` value into a
TIMESTAMP WITH TIME ZONE column, for example) are an hour off.
Set a region name as the session time zone to have that right, too.

Godror will print a

    godor WARNING: discrepancy between SESSIONTIMEZONE and SYSTIMESTAMP

warning when the session time zone differs from the DB's OS' time zone
(`TO_CHAR(SYSTIMESTAMP, 'TZR')`): a DATE filled with SYSDATE is in the latter,
but is read in the former.

## How to eliminate this warning ?
Either use the DB's OS' time zone as the session time zone with `ORA_SDTZ` or

    ALTER SESSION SET TIME_ZONE='Europe/Berlin'

or set one chosen timezone in the [./connection.md](connection string):

    timezone="Europe/Berlin"

(it is parsed with `time.LoadLocation`, so such names can be used,
 or a numeric `+0500` fixed zone; `local` is the same as not setting it).

This `timezone` overrides the session time zone for godror's conversions,
but the server does not know about it: set the session time zone to the same.

*WARNING:* the time zone is cached per DB (and `alterSession`, `onInit` connection parameters),
and it is not read again after an `ALTER SESSION SET TIME_ZONE` executed later on the connection,
so either always ALTER SESSION consistently to the same timezone
(with the `alterSession` connection parameter, for example),
or use the 

    perSessionTimezone=1

connection parameter, to force checking the time zone for each session
(and not cache it per DB).

## Why do we need to handle time zones for DATE ?
DATEs should use something else than time.Time, as the don't have a time zone.
Only TIMESTAMP WITH TIME (LOCAL) TIMEZONE data types should use time.Time.

That'd mean we push the burden of choosing the right time zone to the developer, 
and the std lib (database/sql/driver.Value) mandates the management of time.Time, 
thus either we error out at runtime when we get a time.Time, or manage it somehow.

That's why we have to use time.Time, and deal with time zones.
