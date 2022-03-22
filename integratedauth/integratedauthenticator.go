package integratedauth

// Provider returns an SSPI compatible authentication provider
type Provider interface {
	// GetIntegratedAuthenticator is responsible for returning an instance of the required IntegratedAuthenticator interface
	GetIntegratedAuthenticator(user, password, service, workstation string) (IntegratedAuthenticator, bool)
}

// IntegratedAuthenticator is the interface for SSPI Login Authentication providers
type IntegratedAuthenticator interface {
	InitialBytes() ([]byte, error)
	NextBytes([]byte) ([]byte, error)
	Free()
}

// ProviderFunc is an adapter to convert a GetIntegratedAuthenticator func into a Provider
type ProviderFunc func(user, password, service, workstation string) (IntegratedAuthenticator, bool)

func (f ProviderFunc) GetIntegratedAuthenticator(user, password, service, workstation string) (IntegratedAuthenticator, bool) {
	return f(user, password, service, workstation)
}
