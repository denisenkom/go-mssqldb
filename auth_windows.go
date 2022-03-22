// +build windows

package mssql

import "github.com/denisenkom/go-mssqldb/integratedauth/winsspi"

func init() {
	defaultAuthProvider = winsspi.AuthProvider
}
