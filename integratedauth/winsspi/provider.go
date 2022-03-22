// +build windows

package winsspi

import "github.com/denisenkom/go-mssqldb/integratedauth"

// AuthProvider handles SSPI Windows Authentication via secur32.dll functions
var AuthProvider integratedauth.Provider = integratedauth.ProviderFunc(getAuth)
