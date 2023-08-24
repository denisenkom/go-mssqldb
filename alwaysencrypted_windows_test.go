//go:build go1.17
// +build go1.17

package mssql

import (
	"fmt"
	"testing"

	"github.com/microsoft/go-mssqldb/aecmk"
	"github.com/microsoft/go-mssqldb/aecmk/localcert"
	"github.com/microsoft/go-mssqldb/internal/certs"
	"github.com/stretchr/testify/assert"
)

type certStoreProviderTest struct {
	thumbprint string
}

func (p *certStoreProviderTest) ProvisionMasterKey(t *testing.T) string {
	t.Helper()
	thumbprint, err := certs.ProvisionMasterKeyInCertStore()
	assert.NoError(t, err, "Create cert in cert store")
	certPath := fmt.Sprintf(`CurrentUser/My/%s`, thumbprint)
	p.thumbprint = thumbprint
	return certPath
}

func (p *certStoreProviderTest) DeleteMasterKey(t *testing.T) {
	t.Helper()
	certs.DeleteMasterKeyCert(p.thumbprint)
}

func (p *certStoreProviderTest) GetProvider(t *testing.T) aecmk.ColumnEncryptionKeyProvider {
	t.Helper()
	return &localcert.WindowsCertificateStoreKeyProvider
}

func (p *certStoreProviderTest) Name() string {
	return aecmk.CertificateStoreKeyProvider
}

func init() {
	addProviderTest(&certStoreProviderTest{})
}
