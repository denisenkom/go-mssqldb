package mssql

import (
	"errors"
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
	ptr, _, callErr := initSecurityInterface.Call()
	if callErr != syscall.Errno(0) {
		panic(callErr)
	}
	sec_fn = (*SecurityFunctionTable)(unsafe.Pointer(ptr))
}

const (
	SEC_E_OK             = 0
	SECPKG_CRED_OUTBOUND = 2
)

type SecurityFunctionTable struct {
	dwVersion                  uintptr
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
		Service:  service,
	}, true
}

func (auth *SSPIAuth) InitialBytes() ([]byte, error) {
	/*
		sec_ok, _, callErr := syscall.Syscall9(sec_fn.AcquireCredentialsHandle,
			9,
			0,
			uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr("Negotiate"))),
			SECPKG_CRED_OUTBOUND,
			0,
			0, //TODO identity
			0,
			0,
			0, //TODO credentials
			0) //TODO timestamp
		if callErr != 0 {
			return nil, errors.New("AcquireCredentialsHandle failed")
		}
		if sec_ok != SEC_E_OK {
			return nil, fmt.Errorf("AcquireCredentialsHandle returned %d", sec_ok)
		}
	*/
	return nil, errors.New("SSPI is not implemented")
}

func (auth *SSPIAuth) NextBytes(bytes []byte) ([]byte, error, bool) {
	return nil, errors.New("SSPI is not implemented"), false
}
