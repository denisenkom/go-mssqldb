//go:build go1.17
// +build go1.17

package localcert

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/microsoft/go-mssqldb/aecmk"
	ae "github.com/microsoft/go-mssqldb/internal/github.com/swisscom/mssql-always-encrypted/pkg"
	pkcs "golang.org/x/crypto/pkcs12"
	"golang.org/x/text/encoding/unicode"
)

const (
	PfxKeyProviderName = "pfx"
	wildcard           = "*"
)

// Provider uses local certificates to decrypt CEKs
// It supports both 'MSSQL_CERTIFICATE_STORE' and 'pfx' key stores.
// MSSQL_CERTIFICATE_STORE key paths are of the form `storename/storepath/thumbprint` and only supported on Windows clients.
// pfx key paths are absolute file system paths that are operating system dependent.
type Provider struct {
	// Name identifies which key store the provider supports.
	name string
	// AllowedLocations constrains which locations the provider will use to find certificates. If empty, all locations are allowed.
	// When presented with a key store path not in the allowed list, the data will be returned still encrypted.
	AllowedLocations []string
	passwords        map[string]string
}

// SetCertificatePassword stores the password associated with the certificate at the given location.
// If location is empty the given password applies to all certificates that have not been explicitly assigned a value.
func (p Provider) SetCertificatePassword(location string, password string) {
	if location == "" {
		location = wildcard
	}
	p.passwords[location] = password
}

var PfxKeyProvider = Provider{name: PfxKeyProviderName, passwords: make(map[string]string), AllowedLocations: make([]string, 0)}

func init() {
	err := aecmk.RegisterCekProvider("pfx", &PfxKeyProvider)
	if err != nil {
		panic(err)
	}
}

// DecryptColumnEncryptionKey decrypts the specified encrypted value of a column encryption key.
// The encrypted value is expected to be encrypted using the column master key with the specified key path and using the specified algorithm.
func (p *Provider) DecryptColumnEncryptionKey(masterKeyPath string, encryptionAlgorithm string, encryptedCek []byte) (decryptedKey []byte) {
	decryptedKey = nil
	pk, cert, allowed := p.tryLoadCertificate(masterKeyPath)
	if !allowed {
		return
	}
	cekv := ae.LoadCEKV(encryptedCek)
	if !cekv.Verify(cert) {
		panic(fmt.Errorf("Invalid certificate provided for decryption. Key Store Path: %s. <%s>-<%v>", masterKeyPath, cekv.KeyPath, fmt.Sprintf("%02x", sha1.Sum(cert.Raw))))
	}

	decryptedKey, err := cekv.Decrypt(pk.(*rsa.PrivateKey))
	if err != nil {
		panic(err)
	}
	return
}

func (p *Provider) tryLoadCertificate(masterKeyPath string) (privateKey interface{}, cert *x509.Certificate, allowed bool) {
	allowed = len(p.AllowedLocations) == 0
	if !allowed {
	loop:
		for _, l := range p.AllowedLocations {
			if l == masterKeyPath {
				allowed = true
				break loop
			}
		}
	}
	if !allowed {
		return
	}
	switch p.name {
	case PfxKeyProviderName:
		privateKey, cert = p.loadLocalCertificate(masterKeyPath)
	case aecmk.CertificateStoreKeyProvider:
		privateKey, cert = p.loadWindowsCertStoreCertificate(masterKeyPath)
	}
	return
}

func (p *Provider) loadLocalCertificate(path string) (privateKey interface{}, cert *x509.Certificate) {
	if f, err := os.Open(path); err == nil {
		pfxBytes, err := ioutil.ReadAll(f)
		if err != nil {
			panic(invalidCertificatePath(path, err))
		}
		pwd, ok := p.passwords[path]
		if !ok {
			pwd, ok = p.passwords[wildcard]
			if !ok {
				pwd = ""
			}
		}
		privateKey, cert, err = pkcs.Decode(pfxBytes, pwd)
		if err != nil {
			panic(err)
		}
	} else {
		panic(invalidCertificatePath(path, err))
	}
	return
}

// EncryptColumnEncryptionKey encrypts a column encryption key using the column master key with the specified key path and using the specified algorithm.
func (p *Provider) EncryptColumnEncryptionKey(masterKeyPath string, encryptionAlgorithm string, cek []byte) []byte {

	validateEncryptionAlgorithm(encryptionAlgorithm)
	validateKeyPathLength(masterKeyPath)
	pk, cert, allowed := p.tryLoadCertificate(masterKeyPath)
	if !allowed {
		panic(fmt.Errorf("Key path not allowed for use in column key encryption"))
	}
	publicKey := cert.PublicKey.(*rsa.PublicKey)
	keySizeInBytes := publicKey.Size()

	enc := unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewEncoder()
	// Start with version byte == 1
	buf := []byte{byte(1)}
	// EncryptedColumnEncryptionKey = version + keyPathLength + ciphertextLength + keyPath + ciphertext +  signature
	// version
	keyPathBytes, err := enc.Bytes([]byte(strings.ToLower(masterKeyPath)))
	if err != nil {
		panic(fmt.Errorf("Unable to serialize key path %w", err))
	}
	k := uint16(len(keyPathBytes))
	// keyPathLength
	buf = append(buf, byte(k), byte(k>>8))

	cipherText, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, publicKey, cek, []byte{})
	if err != nil {
		panic(fmt.Errorf("Unable to encrypt data %w", err))
	}
	l := uint16(len(cipherText))
	// ciphertextLength
	buf = append(buf, byte(l), byte(l>>8))
	// keypath
	buf = append(buf, keyPathBytes...)
	// ciphertext
	buf = append(buf, cipherText...)
	hash := sha256.Sum256(buf)
	// signature is the signed hash of the current buf
	sig, err := rsa.SignPKCS1v15(rand.Reader, pk.(*rsa.PrivateKey), crypto.SHA256, hash[:])
	if err != nil {
		panic(err)
	}
	if len(sig) != keySizeInBytes {
		panic("Signature length doesn't match certificate key size")
	}
	buf = append(buf, sig...)
	return buf
}

// SignColumnMasterKeyMetadata digitally signs the column master key metadata with the column master key
// referenced by the masterKeyPath parameter. The input values used to generate the signature should be the
// specified values of the masterKeyPath and allowEnclaveComputations parameters. May return an empty slice if not supported.
func (p *Provider) SignColumnMasterKeyMetadata(masterKeyPath string, allowEnclaveComputations bool) []byte {
	return nil
}

// VerifyColumnMasterKeyMetadata verifies the specified signature is valid for the column master key
// with the specified key path and the specified enclave behavior. Return nil if not supported.
func (p *Provider) VerifyColumnMasterKeyMetadata(masterKeyPath string, allowEnclaveComputations bool) *bool {
	return nil
}

// KeyLifetime is an optional Duration. Keys fetched by this provider will be discarded after their lifetime expires.
// If it returns nil, the keys will expire based on the value of ColumnEncryptionKeyLifetime.
// If it returns zero, the keys will not be cached.
func (p *Provider) KeyLifetime() *time.Duration {
	return nil
}

func validateEncryptionAlgorithm(encryptionAlgorithm string) {
	if !strings.EqualFold(encryptionAlgorithm, "RSA_OAEP") {
		panic(fmt.Errorf("Unsupported encryption algorithm %s", encryptionAlgorithm))
	}
}

func validateKeyPathLength(keyPath string) {
	if len(keyPath) > 32767 {
		panic(fmt.Errorf("Key path is too long. %d", len(keyPath)))
	}
}

// InvalidCertificatePathError indicates the provided path could not be used to load a certificate
type InvalidCertificatePathError struct {
	path     string
	innerErr error
}

func (i *InvalidCertificatePathError) Error() string {
	return fmt.Sprintf("Invalid certificate path: %s", i.path)
}

func (i *InvalidCertificatePathError) Unwrap() error {
	return i.innerErr
}

func invalidCertificatePath(path string, err error) error {
	return &InvalidCertificatePathError{path: path, innerErr: err}
}

func thumbprintToByteArray(thumbprint string) []byte {
	if len(thumbprint)%2 != 0 {
		panic(fmt.Errorf("Thumbprint must have even length %s", thumbprint))
	}
	bytes := make([]byte, len(thumbprint)/2)
	for i := range bytes {
		b, err := strconv.ParseInt(thumbprint[i*2:(i*2)+2], 16, 32)
		if err != nil {
			panic(err)
		}
		bytes[i] = byte(b)
	}
	return bytes
}
