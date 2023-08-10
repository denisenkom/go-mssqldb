//go:build go1.17
// +build go1.17

package localcert

import (
	"crypto/rsa"
	"strings"
	"testing"

	"github.com/microsoft/go-mssqldb/aecmk"
	"github.com/microsoft/go-mssqldb/internal/certs"
)

func TestLoadWindowsCertStoreCertificate(t *testing.T) {
	thumbprint, err := certs.ProvisionMasterKeyInCertStore()
	if err != nil {
		t.Fatal(err)
	}
	defer certs.DeleteMasterKeyCert(thumbprint)
	provider := aecmk.GetGlobalCekProviders()[aecmk.CertificateStoreKeyProvider].Provider.(*Provider)
	pk, cert := provider.loadWindowsCertStoreCertificate("CurrentUser/My/" + thumbprint)
	switch z := pk.(type) {
	case *rsa.PrivateKey:

		t.Logf("Got an rsa.PrivateKey with size %d", z.Size())
	default:
		t.Fatalf("Unexpected private key type: %v", z)
	}
	if !strings.HasPrefix(cert.Subject.String(), `CN=gomssqltest-`) {
		t.Fatalf("Wrong cert loaded: %s", cert.Subject.String())
	}
}

func TestEncryptDecryptEncryptionKeyRoundTrip(t *testing.T) {
	thumbprint, err := certs.ProvisionMasterKeyInCertStore()
	if err != nil {
		t.Fatal(err)
	}
	defer certs.DeleteMasterKeyCert(thumbprint)
	bytesToEncrypt := []byte{1, 2, 3}
	keyPath := "CurrentUser/My/" + thumbprint
	provider := aecmk.GetGlobalCekProviders()[aecmk.CertificateStoreKeyProvider].Provider.(*Provider)
	encryptedBytes := provider.EncryptColumnEncryptionKey(keyPath, "RSA_OAEP", bytesToEncrypt)
	decryptedBytes := provider.DecryptColumnEncryptionKey(keyPath, "RSA_OAEP", encryptedBytes)
	if len(decryptedBytes) != 3 || decryptedBytes[0] != 1 || decryptedBytes[1] != 2 || decryptedBytes[2] != 3 {
		t.Fatalf("Encrypt/Decrypt did not roundtrip. encryptedBytes:%v, decryptedBytes: %v", encryptedBytes, decryptedBytes)
	}
}
