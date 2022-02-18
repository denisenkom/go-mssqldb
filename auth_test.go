package mssql

import (
	"testing"

	"github.com/denisenkom/go-mssqldb/auth"
)

type stubAuth struct {
	user, password, service, workstation string
}

func (s *stubAuth) InitialBytes() ([]byte, error)    { return nil, nil }
func (s *stubAuth) NextBytes([]byte) ([]byte, error) { return nil, nil }
func (s *stubAuth) Free()                            {}

type stubProvider struct {
}

func (p *stubProvider) GetAuth(user, password, service, workstation string) (auth.Auth, bool) {
	return &stubAuth{user, password, service, workstation}, true
}

func TestSetAuthProviderReturnsCorrectInstance(t *testing.T) {
	p := &stubProvider{}

	SetAuthProvider(p)

	if authProvider != p {
		t.Errorf("SetAuthProvider() authProvider: %v, want %v", authProvider, p)
	}

	SetAuthProvider(nil)
}

func TestSetAuthProviderInstanceIsPassedValues(t *testing.T) {
	p := &stubProvider{}

	SetAuthProvider(p)

	result, ok := getAuth("username", "password", "service", "workstation")

	if !ok {
		t.Errorf("expected getAuth() to return ok")
	}

	a, ok := result.(*stubAuth)
	if !ok {
		t.Errorf("expected result of getAuth() to be an instance of stubAuth")
	}

	if a.user != "username" {
		t.Errorf("expected stubAuth username to be correct")
	}

	SetAuthProvider(nil)
}

func TestSetAuthProviderInstanceIsDefaultWhenNil(t *testing.T) {

	SetAuthProvider(nil)

	result, ok := getAuth("username", "password", "service", "workstation")

	_, ok = result.(*stubAuth)
	if ok {
		t.Errorf("expected result of getAuth() to not be an instance of stubAuth")
	}
}
