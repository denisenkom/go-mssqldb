package mssql

import (
	"testing"

	"github.com/denisenkom/go-mssqldb/integratedauth"
)

type stubAuth struct {
	user, password, service, workstation string
}

func (s *stubAuth) InitialBytes() ([]byte, error)    { return nil, nil }
func (s *stubAuth) NextBytes([]byte) ([]byte, error) { return nil, nil }
func (s *stubAuth) Free()                            {}

type stubProvider struct {
}

func (p *stubProvider) GetIntegratedAuthenticator(user, password, service, workstation string) (integratedauth.IntegratedAuthenticator, bool) {
	return &stubAuth{user, password, service, workstation}, true
}

func TestSetIntegratedAuthenticationProviderReturnsCorrectInstance(t *testing.T) {
	p := &stubProvider{}

	SetIntegratedAuthenticationProvider(p)

	if authProvider != p {
		t.Errorf("SetIntegratedAuthenticationProvider() authProvider: %v, want %v", authProvider, p)
	}

	SetIntegratedAuthenticationProvider(nil)
}

func TestSetIntegratedAuthenticationProviderInstanceIsPassedValues(t *testing.T) {
	p := &stubProvider{}

	SetIntegratedAuthenticationProvider(p)

	result, ok := getIntegratedAuthenticator("username", "password", "service", "workstation")

	if !ok {
		t.Errorf("expected getIntegratedAuthenticator() to return ok")
	}

	a, ok := result.(*stubAuth)
	if !ok {
		t.Errorf("expected result of getIntegratedAuthenticator() to be an instance of stubAuth")
	}

	if a.user != "username" {
		t.Errorf("expected stubAuth username to be correct")
	}

	SetIntegratedAuthenticationProvider(nil)
}

func TestSetIntegratedAuthenticationProviderInstanceIsDefaultWhenNil(t *testing.T) {

	SetIntegratedAuthenticationProvider(nil)

	result, ok := getIntegratedAuthenticator("username", "password", "service", "workstation")

	_, ok = result.(*stubAuth)
	if ok {
		t.Errorf("expected result of getIntegratedAuthenticator() to not be an instance of stubAuth")
	}
}
