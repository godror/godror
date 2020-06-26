# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

