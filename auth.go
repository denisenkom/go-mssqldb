package mssql

import (
	"github.com/denisenkom/go-mssqldb/auth"
	"github.com/denisenkom/go-mssqldb/auth/ntlm"
)

var (
	// this instance will be used if one is provided via a call to SetAuthProvider.
	authProvider auth.Provider
	// this is the default implementation to be used when no override is provided by
	// the application. This default is itself overridden to use Windows SSPI in auth_windows.go
	defaultAuthProvider = ntlm.AuthProvider
)

// getAuth calls the authProvider GetAuth if set, otherwise fails back to the
// defaultAuthProvider GetAuth.
func getAuth(user, password, service, workstation string) (auth.Auth, bool) {
	if authProvider != nil {
		return authProvider.GetAuth(user, password, service, workstation)
	}

	return defaultAuthProvider.GetAuth(user, password, service, workstation)
}

// SetAuthProvider allows overriding of the authentication provider used. It should be called before any connections
// are created.
func SetAuthProvider(p auth.Provider) {
	authProvider = p
}
