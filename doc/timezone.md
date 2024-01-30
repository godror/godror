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

As I don't use CURRENT_DATE, I pick the OS' time zone.

Godror will print a

    godor WARNING: discrepancy between SESSIONTIMEZONE and SYSTIMESTAMP

warning that it chosen the DB's OS' time zone (`TO_CHAR(SYSTIMESTAMP, 'TZR')`),
- as that's what SYSDATE is in - but that differs from SESSIONTIMEZONE.

## How to eliminate this warning ?
Either speak with your DBA to synchronize the 
DB's time zone (DBTIMEZONE) with the underlying OS' time zone,
or use 

    ALTER SESSION SET TIME_ZONE='Europe/Berlin'

or set one chosen timezone in the [./connection.md](connection string):

    timezone="Europe/Berlin"

(it is parsed with `time.LoadLocation`, so such names can be used,
 or `local`, or a numeric `+0500` fixed zone).

*WARNING:* time zone altered with `ALTER SESSION` may not be read each and every time,
so either always ALTER SESSION consistently to the same timezone,
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
