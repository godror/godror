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
