//go:build go1.17
// +build go1.17

package localcert

import (
	"crypto/x509"
	"fmt"
)

func (p *Provider) loadWindowsCertStoreCertificate(path string) (privateKey interface{}, cert *x509.Certificate, err error) {
	err = fmt.Errorf("Windows cert store not supported on this OS")
	return
}
