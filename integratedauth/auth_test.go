package integratedauth

import (
	"errors"
	"fmt"
	"testing"

	"github.com/microsoft/go-mssqldb/msdsn"
)

const providerName = "stub"

type stubAuth struct {
	user string
}

func (s *stubAuth) InitialBytes() ([]byte, error)    { return nil, nil }
func (s *stubAuth) NextBytes([]byte) ([]byte, error) { return nil, nil }
func (s *stubAuth) Free()                            {}

func getAuth(config msdsn.Config) (IntegratedAuthenticator, error) {
	return &stubAuth{config.User}, nil
}

func TestSetIntegratedAuthenticationProviderReturnsErrOnNilProvider(t *testing.T) {
	err := SetIntegratedAuthenticationProvider(providerName, nil)

	if err != ErrProviderCannotBeNil {
		t.Errorf("SetIntegratedAuthenticationProvider() returned err: %v, want %v", err, ErrProviderCannotBeNil)
	}
}

func TestSetIntegratedAuthenticationProviderReturnsErrOnEmptyProviderName(t *testing.T) {
	err := SetIntegratedAuthenticationProvider("", ProviderFunc(getAuth))

	if err != ErrProviderNameMustBePopulated {
		t.Errorf("SetIntegratedAuthenticationProvider() returned err: %v, want %v", err, ErrProviderNameMustBePopulated)
	}
}

func TestSetIntegratedAuthenticationProviderStored(t *testing.T) {
	err := SetIntegratedAuthenticationProvider(providerName, ProviderFunc(getAuth))
	if err != nil {
		t.Errorf("SetIntegratedAuthenticationProvider() returned unexpected err %v", err)
	}
	defer removeStubProvider()

	if _, ok := providers[providerName]; !ok {
		t.Error("SetIntegratedAuthenticationProvider() added provider not found")
	}
}

func TestSetIntegratedAuthenticationProviderInstanceIsPassedConnString(t *testing.T) {
	err := SetIntegratedAuthenticationProvider(providerName, ProviderFunc(getAuth))
	if err != nil {
		t.Errorf("SetIntegratedAuthenticationProvider() returned unexpected err %v", err)
	}
	defer removeStubProvider()

	config, err := msdsn.Parse(fmt.Sprintf("authenticator=%v;user id=username", providerName))
	if err != nil {
		t.Errorf("msdsn.Parse : Unexpected error %v", err)
		return
	}

	authenticator, err := GetIntegratedAuthenticator(config)

	if err != nil {
		t.Errorf("expected GetIntegratedAuthenticator() to return ok, found %v", err)
	}

	a, ok := authenticator.(*stubAuth)
	if !ok {
		t.Errorf("expected result of GetIntegratedAuthenticator() to be an instance of stubAuth")
	}

	if a.user != "username" {
		t.Errorf("expected stubAuth username to be correct")
	}
}

func TestSetIntegratedAuthenticationProviderInstanceIsDefaultWhenAuthenticatorParamNotPassed(t *testing.T) {
	removeStubProvider()

	config, err := msdsn.Parse("user id=username")
	if err != nil {
		t.Errorf("msdsn.Parse : Unexpected error %v", err)
		return
	}

	DefaultProviderName = "DEFAULT_PROVIDER"
	defer func() { DefaultProviderName = "" }()

	err = SetIntegratedAuthenticationProvider(DefaultProviderName, ProviderFunc(func(config msdsn.Config) (IntegratedAuthenticator, error) {
		return &stubAuth{"DEFAULT INSTANCE"}, nil
	}))
	if err != nil {
		t.Errorf("SetIntegratedAuthenticationProvider() returned unexpected err %v", err)
	}

	result, err := GetIntegratedAuthenticator(config)

	if err != nil {
		t.Errorf("expected GetIntegratedAuthenticator() to return ok, found %v", err)
	}

	a, ok := result.(*stubAuth)
	if !ok {
		t.Errorf("expected result of GetIntegratedAuthenticator() to be an instance of stubAuth")
	}

	if a.user != "DEFAULT INSTANCE" {
		t.Errorf("expected GetIntegratedAuthenticator for return DefaultProviderName instance when no authenticator param is passed, found %v", a.user)
	}
}

func TestGetIntegratedAuthenticatorFallBackToSqlAuthOnErrorOfDefaultProvider(t *testing.T) {
	removeStubProvider()

	config, err := msdsn.Parse("user id=username")
	if err != nil {
		t.Errorf("msdsn.Parse : Unexpected error %v", err)
		return
	}

	DefaultProviderName = "DEFAULT_PROVIDER"
	defer func() { DefaultProviderName = "" }()

	err = SetIntegratedAuthenticationProvider(DefaultProviderName, ProviderFunc(func(config msdsn.Config) (IntegratedAuthenticator, error) {
		return nil, errors.New("default authenticator cant continue")
	}))
	if err != nil {
		t.Errorf("SetIntegratedAuthenticationProvider() returned unexpected err %v", err)
	}

	result, err := GetIntegratedAuthenticator(config)

	if err != nil {
		t.Errorf("expected GetIntegratedAuthenticator() to return ok, found %v", err)
	}

	if result != nil {
		t.Errorf("expected GetIntegratedAuthenticator() to return nill authenticator, found %v", result)
	}
}

func TestGetIntegratedAuthenticatorToErrorWhenNoDefaultProviderFound(t *testing.T) {
	removeStubProvider()

	// dont set an authenticator
	config, err := msdsn.Parse("user id=username")
	if err != nil {
		t.Errorf("msdsn.Parse : Unexpected error %v", err)
		return
	}

	DefaultProviderName = "NONEXISTANT_DEFAULT_PROVIDER"
	defer func() { DefaultProviderName = "" }()

	result, err := GetIntegratedAuthenticator(config)

	if err == nil {
		t.Error("expected GetIntegratedAuthenticator() to return error, found nil")
	}

	if result != nil {
		t.Errorf("expected GetIntegratedAuthenticator() to return nill provider, found %v", result)
	}

	if err != nil && err.Error() != "provider NONEXISTANT_DEFAULT_PROVIDER not found" {
		t.Errorf("expected err that default provider was not found, found %v", err)
	}
}

func TestGetIntegratedAuthenticatorToErrorWhenNoSpecifiedProviderFound(t *testing.T) {
	removeStubProvider()
	defer removeStubProvider()

	config, err := msdsn.Parse("authenticator=NONEXISTANTPROVIDER;user id=username")
	if err != nil {
		t.Errorf("msdsn.Parse : Unexpected error %v", err)
		return
	}

	// dont set an authenticator
	result, err := GetIntegratedAuthenticator(config)

	if err == nil {
		t.Error("expected GetIntegratedAuthenticator() to return error, found nil")
	}

	if result != nil {
		t.Errorf("expected GetIntegratedAuthenticator() to return nill provider, found %v", result)
	}

	if err != nil && err.Error() != "provider NONEXISTANTPROVIDER not found" {
		t.Errorf("expected err that default provider was not found, found %v", err)
	}
}

func removeStubProvider() {
	delete(providers, providerName)
}
