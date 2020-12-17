package azuread

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io/ioutil"
	"math/big"
	"os"
	"testing"
	"time"
)

func TestValidateParameters(t *testing.T) {
	passphrase := "SuperSecret7"
	certBlock, _, encryptedKeyBlock, err := generateTestCertAndKey(passphrase)
	if err != nil {
		t.Logf("Unable to generate certificate and keys: %v", err)
		t.FailNow()
	}

	pemFile, err := writePEMBlocksToFile([]*pem.Block{certBlock, encryptedKeyBlock})
	if err != nil {
		t.Logf("Unable to write certificate and encrypted key to temporary file: %v", err)
		t.FailNow()
	}

	defer func() {
		os.Remove(pemFile)
	}()

	tests := []struct {
		name     string
		params   map[string]string
		expected *azureFedAuthConfig
	}{
		{
			name:     "no fed auth configured",
			params:   map[string]string{},
			expected: &azureFedAuthConfig{fedAuthLibrary: fedAuthLibraryReserved},
		},
		{
			name: "application with cert/key",
			params: map[string]string{
				"fedauth":        "ActiveDirectoryApplication",
				"user id":        "service-principal-id@tenant-id",
				"password":       passphrase,
				"clientcertpath": pemFile,
			},
			expected: &azureFedAuthConfig{
				fedAuthLibrary: fedAuthLibrarySecurityToken,
				clientID:       "service-principal-id",
				tenantID:       "tenant-id",
				certificate:    &x509.Certificate{},
				privateKey:     &rsa.PrivateKey{},
			},
		},
		{
			name: "application with cert/key missing passphrase",
			params: map[string]string{
				"fedauth":        "ActiveDirectoryApplication",
				"user id":        "service-principal-id@tenant-id",
				"clientcertpath": pemFile,
			},
			expected: nil,
		},
		{
			name: "application with cert/key missing tenant id",
			params: map[string]string{
				"fedauth":        "ActiveDirectoryApplication",
				"user id":        "service-principal-id",
				"password":       passphrase,
				"clientcertpath": pemFile,
			},
			expected: nil,
		},
		{
			name: "application with secret",
			params: map[string]string{
				"fedauth":  "ActiveDirectoryApplication",
				"user id":  "service-principal-id@tenant-id",
				"password": passphrase,
			},
			expected: &azureFedAuthConfig{
				fedAuthLibrary: fedAuthLibrarySecurityToken,
				clientID:       "service-principal-id",
				tenantID:       "tenant-id",
				clientSecret:   passphrase,
			},
		},
		{
			name: "user with password",
			params: map[string]string{
				"fedauth":  "ActiveDirectoryPassword",
				"user id":  "azure-ad-user@example.com",
				"password": "azure-ad-password",
			},
			expected: &azureFedAuthConfig{
				fedAuthLibrary: fedAuthLibraryADAL,
				adalWorkflow:   fedAuthADALWorkflowPassword,
				user:           "azure-ad-user@example.com",
				password:       "azure-ad-password",
			},
		},
		{
			name: "managed identity without client id",
			params: map[string]string{
				"fedauth": "ActiveDirectoryMSI",
			},
			expected: &azureFedAuthConfig{
				fedAuthLibrary: fedAuthLibraryADAL,
				adalWorkflow:   fedAuthADALWorkflowMSI,
			},
		},
		{
			name: "managed identity with client id",
			params: map[string]string{
				"fedauth": "ActiveDirectoryMSI",
				"user id": "identity-client-id",
			},
			expected: &azureFedAuthConfig{
				fedAuthLibrary: fedAuthLibraryADAL,
				adalWorkflow:   fedAuthADALWorkflowMSI,
				clientID:       "identity-client-id",
			},
		},
	}

	for _, tst := range tests {
		config, err := validateParameters(tst.params)
		if tst.expected == nil {
			if err == nil {
				t.Errorf("No error returned when error expected in test case '%s'", tst.name)
			}
			continue
		}

		if err != nil {
			t.Errorf("Error returned when none expected in test case '%s': %v", tst.name, err)
			continue
		}

		if tst.expected.certificate != nil && config.certificate != nil {
			config.certificate = tst.expected.certificate
		}

		if tst.expected.privateKey != nil && config.privateKey != nil {
			config.privateKey = tst.expected.privateKey
		}

		if *config != *tst.expected {
			t.Errorf("Captured parameters do not match in test case '%s'", tst.name)
		}
	}
}

func TestGetFedAuthClientCertificate(t *testing.T) {
	passphrase := "SuperSecret7"
	certBlock, keyBlock, encryptedKeyBlock, err := generateTestCertAndKey(passphrase)
	if err != nil {
		t.Logf("Unable to generate certificate and keys: %v", err)
		t.FailNow()
	}

	expectValid := func(name string) func(*x509.Certificate, *rsa.PrivateKey, error) {
		return func(cert *x509.Certificate, key *rsa.PrivateKey, err error) {
			if err != nil {
				t.Errorf("Error loading %s test case certificate and key: %v", name, err)
			} else {
				if cert == nil {
					t.Errorf("Expected cert but found nil in %s test case", name)
				}

				if key == nil {
					t.Errorf("Expected key but found nil in %s test case", name)
				}
			}
		}
	}

	expectError := func(name string) func(*x509.Certificate, *rsa.PrivateKey, error) {
		return func(cert *x509.Certificate, key *rsa.PrivateKey, err error) {
			if err == nil {
				t.Errorf("Did not get expected error while loading %s test case certificate and key", name)
			}
		}
	}

	tests := []struct {
		name           string
		blocks         []*pem.Block
		loadPassphrase string
		verifier       func(certificate *x509.Certificate, privateKey *rsa.PrivateKey, err error)
	}{
		{
			name:           "valid unencrypted",
			blocks:         []*pem.Block{certBlock, keyBlock},
			loadPassphrase: "",
			verifier:       expectValid("unencrypted"),
		},
		{
			name:           "valid encrypted",
			blocks:         []*pem.Block{certBlock, encryptedKeyBlock},
			loadPassphrase: passphrase,
			verifier:       expectValid("encrypted"),
		},
		{
			name:           "empty",
			blocks:         []*pem.Block{},
			loadPassphrase: "",
			verifier:       expectError("empty"),
		},
		{
			name:           "bogus block type",
			blocks:         []*pem.Block{&pem.Block{Type: "HOT GARBAGE", Bytes: []byte("HOTGARBAGE==")}},
			loadPassphrase: "",
			verifier:       expectError("bogus block type"),
		},
		{
			name:           "bogus certificate",
			blocks:         []*pem.Block{&pem.Block{Type: "CERTIFICATE", Bytes: []byte("HOTGARBAGE==")}},
			loadPassphrase: "",
			verifier:       expectError("bogus certificate"),
		},
		{
			name:           "no private key",
			blocks:         []*pem.Block{certBlock},
			loadPassphrase: "",
			verifier:       expectError("no private key"),
		},
		{
			name:           "bogus private key",
			blocks:         []*pem.Block{certBlock, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: []byte("HOTGARBAGE==")}},
			loadPassphrase: "",
			verifier:       expectError("bogus private key"),
		},
		{
			name:           "bogus encrypted private key",
			blocks:         []*pem.Block{certBlock, &pem.Block{Type: "RSA PRIVATE KEY", Headers: map[string]string{"DEK-Info": "AlsoGarbage"}, Bytes: []byte("HOTGARBAGE==")}},
			loadPassphrase: "",
			verifier:       expectError("bogus encrypted private key"),
		},
	}

	for _, tst := range tests {
		pemFile, err := writePEMBlocksToFile(tst.blocks)
		if err != nil {
			t.Logf("Unable to write PEM blocks for test case %s: %v", tst.name, err)
			t.FailNow()
		}

		func() {
			defer func() { os.Remove(pemFile) }()

			cert, key, err := getFedAuthClientCertificate(pemFile, tst.loadPassphrase)

			tst.verifier(cert, key, err)
		}()
	}
}

func generateTestCertAndKey(passphrase string) (*pem.Block, *pem.Block, *pem.Block, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, nil, err
	}

	keyBlock := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)}

	encryptedKeyBlock, err := x509.EncryptPEMBlock(rand.Reader, keyBlock.Type, keyBlock.Bytes, []byte(passphrase), x509.PEMCipherAES256)
	if err != nil {
		return nil, nil, nil, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"go-mssqldb"},
		},
		NotBefore: time.Now().Add(-(time.Minute * 5)),
		NotAfter:  time.Now().Add(time.Hour * 24),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, nil, err
	}

	certBlock := &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}

	return certBlock, keyBlock, encryptedKeyBlock, nil
}

func writePEMBlocksToFile(blocks []*pem.Block) (string, error) {
	f, err := ioutil.TempFile("", "go-mssql-azureauth-")
	if err != nil {
		return "", err
	}

	for _, block := range blocks {
		if err = pem.Encode(f, block); err != nil {
			return "", err
		}
	}

	if err = f.Close(); err != nil {
		return "", err
	}

	return f.Name(), nil
}
