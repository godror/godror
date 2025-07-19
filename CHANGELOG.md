# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.48.1]
### Fixed
- Fixed a nasty silent cgo (usage) bug for Go1.24 that borked object NUMBER retrieval - see https://github.com/godror/godror/issues/365

## [0.48.0]
### Added
- Vector and sparse vector support

## [0.47.1]
### Fixed
- Read NCLOB: must count in UTF-16, and don't split surrogates (ORA-22831)
- Use CallTimeout as context.WithTimeout

## [0.47.0]
### Changed
- ConnectionParams IsSysDBA, IsSysOper, IsSysASM -> AdminRole

## [0.46.2]
### Changed
- Use ODPI-5.5.0

## [0.46.0]
### Changed
- dsn.StandaloneConnection, dsn.Heterogeneous and dsn.ExternalAuth became sql.NullBool to be able to differentiate set and not set.
- Use ODPI-v5.4.1, fixing SIGSEGV on standalone connection with external auth.

## [v0.45.2]
### Added
- NoRetry option to disable re-execution of a statement on ORA-04061, ORA-04065 or ORA-04068
- WarningAsError option to return ORA-24344 as an error instead of skipping it
- SetAttribute that sidesteps ORA-21602

### Changed
- Make CLOB written sizem mismatch only a warning.

## [v0.45.0]
### Added
- Add cloexec subpackage for setting the connections to FD_CLOEXEC.
- Export DPI_NATIVE_TYPE_* constants for Data.NativeType comparisons.

### Changed
- Require Go 1.20 (drop support for Go 1.19)
- ODPI-C v5.4.0

## [v0.44.0]
### Added
- Allow specifying Token authentication
- PartialBatch option for SAVE EXCEPTIONS handling
- Obey to context cancelation (as the documentation says),
not just deadlines
- add noBreakOnContextCancel option as a safety valve it the previous
change cause SIGSEGV.

## [v0.40.3]
### Changed
- Fix compilation regression with Go 1.19, caused by introducing log/slog.

## [v0.40.2]
### Changed
- Fix performance issue of #301.

## [v0.40.0]
### Changed
- ODPI-C v5.0.0

## [v0.39.3]
### Changed
- Make resources' (last resort) Close with Finalizers opt-in with GuardWithFinalizers().
- Remove string/Number interning, as it resulted higher memory usage.
- Simplify DB time zone caching's key.

## [v0.39.2]
### Changed
- simplify handleDeadline, hopefully eliminiate SIGSEGVs

## [v0.39.1]
### Changed
- Update golang.org/x/exp/slog
- Raise 3s default first connection timeout to 10s

## [v0.39.0]
### Changed
- Use ODPI-C v5.0

## [v0.38.0]
### Added
- Auto-start queue on specific failures (if possible)
- NumberAsFloat64 option to return every number as float64

## Changed
- Log with slog (use github.com/godror/godror/slog shim or directly log/slog or golang.org/x/exp/slog)

## [v0.37.0]
### Changed
- ODPI-C v4.6.1

## [v0.36.0]
### Added
- initOnNewConnection connection flag to call OnInit only when the connection is new.

## [v0.35.0]
### Changed
- Use ODPI-v4.6.0

## [v0.34.0]
### Added
- ObjectType.AttributeNames() returns the attribute names in DB order.
- PoolStats has a DBStats method to convert it to sql.DBStats.
- By embedding godror.ObjectTypeName and setting the field tag with `"godror:,type=my_pkg.type"` you can use structs in place of Oracle objects in ExecStmt.

## [v0.33.0]
### Changed
- SetLogger accept github.com/go-logr/logr.Logger

## [v0.32.0]
- Update to Go 1.15 as minimal required version.

## [v0.31.0]
### Added
- NumberAsString option
- Queue.PurgeExpired
### Changed
- By default, return Number for numbers (not string)
- Always run session Init functions

## [v0.30.2]
### Changed
- Fix Queue.Dequeue to work with non-existing MsgID (#201).

## [v0.30.1]
### Added
- Object{,Collection}.FromJSON

## [v0.30.0]
### Added
- Object{,Collection}.{AsMap,ToJSON}

### Changed
- DeqOptions.MsgID changed from string to []byte

## [v0.29.0]
### Added
- Add github.com/godror/knownpb/timestamppb.Timestamp
### Changed
- Remove pbTimestamp (behind timestamppb tag), github.com/UNO-SOFT/knownpb/timestamppb implements driver.Valuer and sql.Scanner.

## [v0.28.1]
### Changed
- ODPI-C v4.3.0
### Added 
- Add NewTempLob to the Conn interface for #189.

## [v0.28.0]
### Changed
- Remove ObjectType.NewData, to get rid of the dependency on *conn in ObjectType, for #172

## [v0.27.1]
### Added
- Size, ReadAt methods to dpiLobReader

## [v0.27.0]
### Changed
- Instead of Log package-level variable, use SetLog or SetLogger.

## [v0.26.0]
### Added
- Batch type for batching ExecContext calls.
- Add support for native JSON type support (for DB21.3)

## [v0.25.6]
### Changed
- Fix lobReader buffering which caused short reads.

## [v0.25.4]
### Added
- Implement use of "google.golang.org/protobuf/types/known/timestamppb".Timestamp,
  behind the timestamppb tag

### Changed
- Use dpiConn_setCallTimeout for faster recovers.
- Faster setTraceTag, drv.init.
- Buffer dpiLobReader.Read with chunk-sized reads for 
  * avoiding io.ReadAll short read and
  * performance

## [v0.25.2]
### Changed
- Go 1.14 minimum requirement

## [v0.25.1]
### Added
- Add stmtCacheSize, poolMaxSessionPerShard, poolPingInterval params
- OnInit use context.Context

### Changed
- Use ODPI-C v4.2.1

## [v0.24.3]
### Added
- IsBadConn to check for the several error numbers all indicate connection failure

## [v0.24.0]
### Added
- noTimezoneCheck flag in connection string to suppress the WARNING printout
- tests use GODROR_TEST_DSN env var

### Changed
- GetCompileErrors requires context.Context.
- ObjectType became a pointer (uses a mutex, must be a pointer)

## [v0.23.1]
### Added
- NewDriver() to return a new driver - and drv.Close() method.

### Changed
- Fix NewPassword handling to allow password change.

## [v0.23.0]
### Changed
- All OCI/ODPI calls encapsulated in runtime.LockOSThread / runtime.UnlockOSThread
to force the error retrieving be on the same OS thread - it seems that OCI stores
the last error on some kind of thead-local-storage.

## [v0.22.0]
### Added
- doc/timezone.md for documentation about time zones

### Changed
- Set DefaultPrefetchCount = DefaultArraySize (=100), from the previous 2.
- Use SESSIONTIMEZONE instead of DBTIMEZONE. 

## [0.21.0]
### Changed
- Use ODPI-C v4.1.0

## [0.20.6]
### Added
- Compose/Decompose implementation for Number and num.OCINum.

## [0.20.5]
### Changed
- Obey context deadlines everyewhere by calling OCIBreak on timeout/cancelation.

## [0.20.1]
### Changed
- Fix Break (context cancelation/timeout) handling for good.

## [0.20.0]
### Added
- GetFileName method to DirectLob.

### Changed
- DeqOptions.Delay, Expiration and Wait became a time.Duration
- Use ODPI-C v4.0.2

## [0.19.4]
### Added
- Allow specifying Enq/DeqOptions in NewQueue.

### Changed
- Changed the default Enq/Deq Queue options to the Oracle defaults.
- Document that Go 1.13 is required (for sql.NullInt32).

## [0.19.2]
### Added
- SELECT ROWID as string (no need for the ''||ROWID workaround).
- Documentation at godror.github.io/godror

## [0.19.1]
### Changed
- Separate documentation under doc/
- Only allow user/passwd@sid for old-style connection strings.

## [0.19.0]
### Changed
- New, logfmt-formatted dataSourceName with connectString included as what's the old connection string.
  As the old format is accepted, this is backward-compatible.
- Rename DSN to ConnectString in ConnectionParams - BACKWARD INCOMPATIBLE CHANGE!

## [0.18.0]
### Changed
- Password, a new type to hide secrets - BACKWARD INCOMPATIBLE CHANGE!

### Fixed
- Timezone getting logic when TZ is not UTC.

## [0.17.5]
### Changed
- Better caching of timezone information

### Added
- Allow uint16, int8, int16, sql.NullInt32 types for Data.Set.

## [0.17.1]
### Added
- Allow specifying OCI lib path and config dir

### Changed
- Fixed TimeZone caching and lock issues.

## [0.17.0]
### Added 
- PrefetchCount statement option to set prefetch row count.

### Changed
- Use ODPI-C v4.0.0
- Deprecate FetchRowCount in favor of FetchArraySize.

## [0.16.1]
### Changed
- Add Finalizer for conn, statement and rows, prints ugly stack trace on forgotten Close()s.

## [0.16.0]
### Added
- Add NullDateAsZeroTime option.
- Add GetPoolStats to Conn to get the pool statistics.

### Changed
- Make standaloneConnection the default - pools have problems.

## [0.15.0]
### Changed
- Innards of ConnectionParams has been split to ConnParams and PoolParams,
- NewConnector needs ConnParams and PoolParams instead of the connection URL.
- ConnectionParams.SID has been renamed to DSN.
- Simplified pool usage and coding, just depend on the underlying ODPI-C library's reference counting.

### Added
- Support connection sharding (thanks to Anthony Tuininga)
- Implement SessionResetter and Validator, releasing session back to the Oracle session pool ASAP,
  acquiring a fresh connection in ResetSession, helping failover.

## [0.14.0]
### Changed
- Make NumberAsString() the default.
- Remove NumberAsString() and MagicConversion() Options.

## [0.13.3]
### Added
- Dummy .go files to allow vendoring with "go mod vendor".

## [0.13.0]
### Changed
- NewSubscription got ...SubscriptionOption optional arguments.

## [0.12.1]
### Changed
- Fix Data.SetTime

## [0.12.0]
### Added
- BoolToString option to convert from bool to string for DML statements.

### Changed
- INTERVAL YEAR TO MONTHS format changes from %dy%dm to %d-%d, as Oracle uses it.

## [0.11.4]
### Added
- Accept time.Duration and insert it as INTERVAL DAY TO SECOND.

## [0.11.0]
### Added
- NullTime to handle NULL DATE columns

### Changed
- Return NullTime instead of time.Time for interface{} destination in column description.

## [0.10.0]
### Added
- onInit parameter in the connection url (and OnInit in ConnectionParams)
- export Drv to be able to register new driver wrapping *godror.Drv.
- ContextWithUserPassw requires a connClass argument, too.

## [0.9.2]
### Changed
- Make Data embed dpiData, not *dpiData

