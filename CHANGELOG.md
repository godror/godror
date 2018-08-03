# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.5.9] - 2018-08-03
### Added
- add CHANGELOG
- check that `len(dest) == len(rows.columns)` in `rows.Next(dest)`

### Changed
- after a Break, don't release a stmt, that may fail with SIGSEGV - see #84.

## [2.5.8] - 2018-07-27
### Changed
- noConnectionPooling option became standaloneConnection

## [2.5.7] - 2018-07-25
### Added
- noConnectionPooling option to force not using a session pool

## [2.5.6] - 2018-07-18
### Changed
- use ODPI-C v2.4.2
- remove all logging/printing of passwords

## [2.5.5] - 2018-07-03
### Added
- allow *int with nil value to be used as NULL

## [2.5.4] - 2018-06-29
### Added
- allow ReadOnly transactions

## [2.5.3] - 2018-06-29
### Changed
- decrease maxArraySize to be compilable on 32-bit architectures.

### Removed
- remove C struct size Printf

## [2.5.2] - 2018-06-22
### Changed
- fix liveness check in statement.Close

## [2.5.1] - 2018-06-15
### Changed
- sid -> service_name in docs
- travis: 1.10.3
- less embedding of structs, clearer API docs

### Added
- support RETURNING from DML
- set timeouts on poolCreateParams

## [2.5.0] - 2018-05-15
### Changed
- update ODPI-C to v2.4.0
- initialize context / load lib only on first Open, to allow import without Oracle Client installed
- use golangci-lint


