package mssql

import (
	"github.com/denisenkom/go-mssqldb/integratedauth"
	"github.com/denisenkom/go-mssqldb/integratedauth/ntlm"
)

var (
	// this instance will be used if one is provided via a call to SetIntegratedAuthenticationProvider.
	authProvider integratedauth.Provider
	// this is the default implementation to be used when no override is provided by
	// the application. This default is itself overridden to use Windows SSPI in auth_windows.go
	defaultAuthProvider = ntlm.AuthProvider
)

// getIntegratedAuthenticator calls the authProvider GetIntegratedAuthenticator if set, otherwise fails back to the
// defaultAuthProvider GetIntegratedAuthenticator.
func getIntegratedAuthenticator(user, password, service, workstation string) (integratedauth.IntegratedAuthenticator, bool) {
	if authProvider != nil {
		return authProvider.GetIntegratedAuthenticator(user, password, service, workstation)
	}

	return defaultAuthProvider.GetIntegratedAuthenticator(user, password, service, workstation)
}

// SetIntegratedAuthenticationProvider allows overriding of the authentication provider used. It should be called before any connections
// are created.
func SetIntegratedAuthenticationProvider(p integratedauth.Provider) {
	authProvider = p
}
