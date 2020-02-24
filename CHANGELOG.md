# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

