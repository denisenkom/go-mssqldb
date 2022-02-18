package ntlm

import "github.com/denisenkom/go-mssqldb/auth"

// AuthProvider handles NTLM SSPI Windows Authentication
var AuthProvider auth.Provider = auth.ProviderFunc(getAuth)
