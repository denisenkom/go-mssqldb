module github.com/denisenkom/go-mssqldb/examples/azuread

go 1.13

require (
	github.com/Azure/go-autorest/autorest/adal v0.8.1
	github.com/denisenkom/go-mssqldb v0.0.0-20191128021309-1d7a30a10f73
)

replace github.com/denisenkom/go-mssqldb => ../..