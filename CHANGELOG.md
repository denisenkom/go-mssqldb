# Changelog

## 0.20.0

### Features

* Add driver version and name to TDS login packets
* Add `pipe` connection string parameter for named pipe dialer

### Bug fixes

* Added checks while reading prelogin for invalid data ([#64](https://github.com/microsoft/go-mssqldb/issues/64))([86ecefd8b](https://github.com/microsoft/go-mssqldb/commit/86ecefd8b57683aeb5ad9328066ee73fbccd62f5))

* Fixed multi-protocol dialer path to avoid unneeded SQL Browser queries