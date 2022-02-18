// +build windows

package mssql

import "github.com/denisenkom/go-mssqldb/auth/winsspi"

func init() {
	defaultAuthProvider = winsspi.AuthProvider
}
