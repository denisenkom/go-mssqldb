// +build !windows

package mssql

import (
	"errors"
	"strings"
)

type NTLMAuth struct {
	Domain   string
	UserName string
	Password string
}

func getAuth(user, password string) (Auth, bool) {
	if !strings.ContainsRune(user, '\\') {
		return nil, false
	}
	domain_user := strings.SplitN(user, "\\", 2)
	return &NTLMAuth{
		Domain:   domain_user[0],
		UserName: domain_user[1],
		Password: password,
	}, true
}

func (auth *NTLMAuth) InitialBytes() ([]byte, error) {
	return nil, errors.New("NTLM is not implemented")
}

func (auth *NTLMAuth) NextBytes(bytes []byte) ([]byte, error, bool) {
	return nil, errors.New("NTLM is not implemented"), false
}
