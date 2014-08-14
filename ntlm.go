// +build !windows

package mssql

import (
	"encoding/binary"
	"errors"
	"strings"
	"unicode/utf16"
)

const (
	NEGOTIATE_MESSAGE    = 1
	CHALLENGE_MESSAGE    = 2
	AUTHENTICATE_MESSAGE = 3
)

const (
	NEGOTIATE_UNICODE              = 0x00000001
	NEGOTIATE_OEM                  = 0x00000002
	NEGOTIATE_TARGET               = 0x00000004
	NEGOTIATE_SIGN                 = 0x00000010
	NEGOTIATE_SEAL                 = 0x00000020
	NEGOTIATE_DATAGRAM             = 0x00000040
	NEGOTIATE_LMKEY                = 0x00000080
	NEGOTIATE_NTLM                 = 0x00000200
	NEGOTIATE_ANONYMOUS            = 0x00000800
	NEGOTIATE_DOMAIN_SUPPLIED      = 0x00001000
	NEGOTIATE_WORKSTATION_SUPPLIED = 0x00002000
	NEGOTIATE_ALWAYS_SIGN          = 0x00008000
	NEGOTIATE_TARGET_TYPE_DOMAIN   = 0x00010000
	NEGOTIATE_TARGET_TYPE_SERVER   = 0x00020000
	NEGOTIATE_EXTENDED_SECURITY    = 0x00080000
	NEGOTIATE_IDENTIFY             = 0x00100000
	REQUEST_NON_NT_SESSION_KEY     = 0x00400000
	NEGOTIATE_TARGET_INFO          = 0x00800000
	NEGOTIATE_VERSION              = 0x02000000
	NEGOTIATE_128                  = 0x20000000
	NEGOTIATE_KEY_EXCH             = 0x400000000
	NEGOTIATE_56                   = 0x80000000
)

const NEGOTIATE_FLAGS = NEGOTIATE_UNICODE |
	NEGOTIATE_NTLM |
	NEGOTIATE_DOMAIN_SUPPLIED |
	NEGOTIATE_ALWAYS_SIGN |
	NEGOTIATE_EXTENDED_SECURITY

type NTLMAuth struct {
	Domain   string
	UserName string
	Password string
}

func getAuth(user, password, service string) (Auth, bool) {
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

func utf16le(val string) []byte {
	var v []byte
	for _, r := range val {
		if utf16.IsSurrogate(r) {
			r1, r2 := utf16.EncodeRune(r)
			v = append(v, byte(r1), byte(r1>>8))
			v = append(v, byte(r2), byte(r2>>8))
		} else {
			v = append(v, byte(r), byte(r>>8))
		}
	}
	return v
}

func (auth *NTLMAuth) InitialBytes() ([]byte, error) {
	domain16 := utf16le(auth.Domain)
	domain_len := len(domain16)
	msg := make([]byte, 40+domain_len)
	copy(msg, []byte("NTLMSSP\x00"))
	binary.LittleEndian.PutUint32(msg[8:], NEGOTIATE_MESSAGE)
	binary.LittleEndian.PutUint32(msg[12:], NEGOTIATE_FLAGS)
	binary.LittleEndian.PutUint16(msg[16:], uint16(domain_len))
	binary.LittleEndian.PutUint16(msg[18:], uint16(domain_len))
	binary.LittleEndian.PutUint32(msg[20:], 40) // domain offset
	binary.LittleEndian.PutUint16(msg[24:], 0)
	binary.LittleEndian.PutUint16(msg[26:], 0)
	binary.LittleEndian.PutUint32(msg[28:], 0) // workstation offset
	binary.LittleEndian.PutUint32(msg[32:], 0) // version
	binary.LittleEndian.PutUint32(msg[36:], 0)
	copy(msg[40:], domain16)
	return msg, nil
}

func (auth *NTLMAuth) NextBytes(bytes []byte) ([]byte, error) {
	return nil, errors.New("NTLM is not implemented")
}

func (auth *NTLMAuth) Free() {
}
