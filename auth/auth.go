package auth

// Provider is an SSPI compatible authentication provider
type Provider interface {
	// GetAuth is responsible for returning an instance of the required Auth interface
	GetAuth(user, password, service, workstation string) (Auth, bool)
}

// Auth is the interface for SSPI Login Authentication providers
type Auth interface {
	InitialBytes() ([]byte, error)
	NextBytes([]byte) ([]byte, error)
	Free()
}

// ProviderFunc is an adapter to convert a GetAuth func into a Provider
type ProviderFunc func(user, password, service, workstation string) (Auth, bool)

func (f ProviderFunc) GetAuth(user, password, service, workstation string) (Auth, bool) {
	return f(user, password, service, workstation)
}
