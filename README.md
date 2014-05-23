# A pure Go MSSQL driver for Go's database/sql package

## Install

    go get github.com/denisenkom/go-mssqldb

## Tests

`go test` is used for testing. A running instance of MSSQL server is required.
Environment variables are used to pass login information.

Example:

    env HOST=localhost SQLUSER=sa SQLPASSWORD=sa DATABASE=test go test

## Connection Parameters

* "server" - host or host\instance
* "port" - used only when there is no instance in server (default 1433)
* "user id"
* "password"
* "database"
* "connection timeout" - in seconds (default is 30)
* "log" - logging flags (default 0/no logging, 63 for full logging)
  *  1 log errors
  *  2 log messages
  *  4 log rows affected
  *  8 trace sql statements
  * 16 log statement parameters
  * 32 log transaction begin/end
* "encrypt"
  * false - Data sent between client and server is not encrypted beyond the login packet. (Default)
  * true - Data sent between client and server is encrypted.
* "trust server certificate"
  * false - Server certificate is checked. Default is false if encypt is specified.
  * true - Server certificate is not checked. Default is true if encrypt is not specified. If trust server cerrtificate is true, driver accepts any certificate presented by the server and any host name in that certificate. In this mode, TLS is susceptible to man-in-the-middle attacks. This should be used only for testing.
* "certificate" - The file that contains the public key certificate of the CA that signed the SQL Server certificate. The specified certificate overrides the go platform specific CA certificates.
* "host in certificate" - Specifies the Common Name (CN) in the server certificate. Default value is the server host. 

Example:

```go
    db, err := sql.Open("mssql", "server=localhost;user id=sa")
```

## Statement Parameters

In the SQL statement text, literals may be replaced by a parameter that matches one of the following:

* ?
* ?nnn
* :nnn
* $nnn

where nnn represents an integer.

## Features

* Can be used with SQL Server 2005 or newer
* Can be used on all go supported platforms (e.g. Linux, Mac OS X and Windows)
* Supports new date/time types: date, time, datetime2, datetimeoffset
* Supports string parameters longer that 8000 characters
* Supports encryption using SSL/TLS
