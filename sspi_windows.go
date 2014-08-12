package mssql

import (
	"errors"
	"fmt"
	"strings"
	"syscall"
	"unsafe"
)

var (
	secur32_dll           = syscall.NewLazyDLL("secur32.dll")
	initSecurityInterface = secur32_dll.NewProc("InitSecurityInterfaceW")
	sec_fn                *SecurityFunctionTable
)

func init() {
	ptr, _, _ := initSecurityInterface.Call()
	sec_fn = (*SecurityFunctionTable)(unsafe.Pointer(ptr))
}

const (
	SEC_E_OK                        = 0
	SECPKG_CRED_OUTBOUND            = 2
	SEC_WINNT_AUTH_IDENTITY_UNICODE = 2
)

type SecurityFunctionTable struct {
	dwVersion                  uint32
	EnumerateSecurityPackages  uintptr
	QueryCredentialsAttributes uintptr
	AcquireCredentialsHandle   uintptr
	FreeCredentialsHandle      uintptr
	Reserved2                  uintptr
	InitializeSecurityContext  uintptr
	AcceptSecurityContext      uintptr
	CompleteAuthToken          uintptr
	DeleteSecurityToken        uintptr
	ApplyControlToken          uintptr
	QueryContextAttributes     uintptr
	ImpersonateSecurityContext uintptr
	RevertSecurityContext      uintptr
	MakeSignature              uintptr
	VerifySignature            uintptr
	FreeContextBuffer          uintptr
	QuerySecurityPackageInfo   uintptr
	Reserved3                  uintptr
	Reserved4                  uintptr
	Reserved5                  uintptr
	Reserved6                  uintptr
	Reserved7                  uintptr
	Reserved8                  uintptr
	QuerySecurityContextToken  uintptr
	EncryptMessage             uintptr
	DecryptMessage             uintptr
}

type SEC_WINNT_AUTH_IDENTITY struct {
	User           *uint16
	UserLength     uint32
	Domain         *uint16
	DomainLength   uint32
	Password       *uint16
	PasswordLength uint32
	Flags          uint32
}

type TimeStamp struct {
	LowPart  uint32
	HighPart int32
}

type SecHandle struct {
	dwLower uint32
	dwUpper uint32
}

type SSPIAuth struct {
	Domain   string
	UserName string
	Password string
	Service  string
	cred     SecHandle
	ctxt     SecHandle
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
		Service:  service,
	}, true
}

func (auth *SSPIAuth) InitialBytes() ([]byte, error) {
	var identity *SEC_WINNT_AUTH_IDENTITY
	if auth.UserName != "" {
		identity = &SEC_WINNT_AUTH_IDENTITY{
			Flags:          SEC_WINNT_AUTH_IDENTITY_UNICODE,
			Password:       syscall.StringToUTF16Ptr(auth.Password),
			PasswordLength: uint32(len(auth.Password)),
			Domain:         syscall.StringToUTF16Ptr(auth.Domain),
			DomainLength:   uint32(len(auth.Domain)),
			User:           syscall.StringToUTF16Ptr(auth.UserName),
			UserLength:     uint32(len(auth.UserName)),
		}
	}
	var ts TimeStamp
	sec_ok, _, _ := syscall.Syscall9(sec_fn.AcquireCredentialsHandle,
		9,
		0,
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr("Negotiate"))),
		SECPKG_CRED_OUTBOUND,
		0,
		uintptr(unsafe.Pointer(identity)),
		0,
		0,
		uintptr(unsafe.Pointer(&auth.cred)),
		uintptr(unsafe.Pointer(&ts)))
	if sec_ok != SEC_E_OK {
		return nil, fmt.Errorf("AcquireCredentialsHandle returned %d", sec_ok)
	}
	syscall.Syscall6(sec_fn.FreeCredentialsHandle,
		1,
		uintptr(unsafe.Pointer(&auth.cred)),
		0, 0, 0, 0, 0)
	return nil, errors.New("SSPI is not implemented")
}

func (auth *SSPIAuth) NextBytes(bytes []byte) ([]byte, error, bool) {
	return nil, errors.New("SSPI is not implemented"), false
}
