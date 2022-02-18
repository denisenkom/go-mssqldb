// +build windows

package winsspi

import "github.com/denisenkom/go-mssqldb/auth"

// AuthProvider handles SSPI Windows Authentication via secur32.dll functions
var AuthProvider auth.Provider = auth.ProviderFunc(getAuth)
