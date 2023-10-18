//go:build go1.17
// +build go1.17

package localcert

import (
	"crypto/x509"
	"fmt"
	"strings"
	"unsafe"

	"github.com/microsoft/go-mssqldb/aecmk"
	"github.com/microsoft/go-mssqldb/internal/certs"
	"golang.org/x/sys/windows"
)

var WindowsCertificateStoreKeyProvider = Provider{name: aecmk.CertificateStoreKeyProvider, passwords: make(map[string]string)}

func init() {
	err := aecmk.RegisterCekProvider(aecmk.CertificateStoreKeyProvider, &WindowsCertificateStoreKeyProvider)
	if err != nil {
		panic(err)
	}
}

func (p *Provider) loadWindowsCertStoreCertificate(path string) (privateKey interface{}, cert *x509.Certificate, err error) {
	privateKey = nil
	cert = nil
	pathParts := strings.Split(path, `/`)
	if len(pathParts) != 3 {
		err = invalidCertificatePath(path, fmt.Errorf("key store path requires 3 segments"))
		return
	}

	var storeId uint32
	switch strings.ToLower(pathParts[0]) {
	case "localmachine":
		storeId = windows.CERT_SYSTEM_STORE_LOCAL_MACHINE
	case "currentuser":
		storeId = windows.CERT_SYSTEM_STORE_CURRENT_USER
	default:
		err = invalidCertificatePath(path, fmt.Errorf("Unknown certificate store"))
		return
	}
	system, err := windows.UTF16PtrFromString(pathParts[1])
	if err != nil {
		err = invalidCertificatePath(path, err)
		return
	}
	h, err := windows.CertOpenStore(windows.CERT_STORE_PROV_SYSTEM,
		windows.PKCS_7_ASN_ENCODING|windows.X509_ASN_ENCODING,
		0,
		storeId, uintptr(unsafe.Pointer(system)))
	if err != nil {
		return
	}
	defer windows.CertCloseStore(h, 0)
	signature := thumbprintToByteArray(pathParts[2])
	return certs.FindCertBySignatureHash(h, signature)
}
