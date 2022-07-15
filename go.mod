module github.com/denisenkom/go-mssqldb

go 1.17

require (
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe
	github.com/pkg/errors v0.9.1
	golang.org/x/crypto v0.0.0-20211215153901-e495a2d5b3d3
)

// Prune insecure versions from the dependency tree
replace golang.org/x/crypto v0.0.0-20211215153901-e495a2d5b3d3 => golang.org/x/crypto v0.0.0-20220314234659-1baeb1ce4c0b

replace golang.org/x/net v0.0.0-20211112202133-69e39bad7dc2 => golang.org/x/net v0.0.0-20211209124913-491a49abca63

replace golang.org/x/text v0.3.6 => golang.org/x/text v0.3.7
