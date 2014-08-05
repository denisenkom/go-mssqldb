package mssql

import (
	"errors"
	"strings"
)

type SSPIAuth struct {
	Domain   string
	UserName string
	Password string
	Service  string
}

func getAuth(user, password, service string) (Auth, bool) {
	if user == "" {
		return &SSPIAuth{Service: service}, true
	}
	if !strings.ContainsRune(user, '\\') {
		return nil, false
	}
	domain_user := strings.SplitN(user, "\\", 2)
	return &SSPIAuth{
		Domain:   domain_user[0],
		UserName: domain_user[1],
		Password: password,
	}, true
}

func (auth *SSPIAuth) InitialBytes() ([]byte, error) {
	return nil, errors.New("SSPI is not implemented")
}

func (auth *SSPIAuth) NextBytes(bytes []byte) ([]byte, error, bool) {
	return nil, errors.New("SSPI is not implemented"), false
}
