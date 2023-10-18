//go:build go1.17
// +build go1.17

package localcert

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"fmt"
	"io"
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
func (p *Provider) DecryptColumnEncryptionKey(ctx context.Context, masterKeyPath string, encryptionAlgorithm string, encryptedCek []byte) (decryptedKey []byte, err error) {
	decryptedKey = nil
	err = validateEncryptionAlgorithm(aecmk.Encryption, encryptionAlgorithm)
	if err != nil {
		return
	}
	err = validateKeyPathLength(aecmk.Encryption, masterKeyPath)
	if err != nil {
		return
	}
	pk, cert, err := p.tryLoadCertificate(aecmk.Decryption, masterKeyPath)
	if err != nil {
		return
	}
	cekv := ae.LoadCEKV(encryptedCek)
	if !cekv.Verify(cert) {
		err = aecmk.NewError(aecmk.Decryption, fmt.Sprintf("Invalid certificate provided for decryption. Key Store Path: %s. <%s>-<%v>", masterKeyPath, cekv.KeyPath, fmt.Sprintf("%02x", sha1.Sum(cert.Raw))), nil)
		return
	}

	decryptedKey, err = cekv.Decrypt(pk.(*rsa.PrivateKey))
	if err != nil {
		err = aecmk.NewError(aecmk.Decryption, fmt.Sprintf("Decryption failed using %s", masterKeyPath), err)
	}
	return
}

func (p *Provider) tryLoadCertificate(op aecmk.Operation, masterKeyPath string) (privateKey interface{}, cert *x509.Certificate, err error) {
	allowed := len(p.AllowedLocations) == 0
	if !allowed {
	loop:
		for _, l := range p.AllowedLocations {
			if strings.HasPrefix(masterKeyPath, l) {
				allowed = true
				break loop
			}
		}
	}
	if !allowed {
		err = aecmk.KeyPathNotAllowed(masterKeyPath, op)
		return
	}
	switch p.name {
	case PfxKeyProviderName:
		privateKey, cert, err = p.loadLocalCertificate(masterKeyPath)
	case aecmk.CertificateStoreKeyProvider:
		privateKey, cert, err = p.loadWindowsCertStoreCertificate(masterKeyPath)
	}
	if err != nil {
		err = aecmk.NewError(op, "Unable to load certificate", err)
	}
	return
}

func (p *Provider) loadLocalCertificate(path string) (privateKey interface{}, cert *x509.Certificate, err error) {
	if f, e := os.Open(path); e == nil {
		pfxBytes, er := io.ReadAll(f)
		if er != nil {
			err = invalidCertificatePath(path, er)
			return
		}
		pwd, ok := p.passwords[path]
		if !ok {
			pwd, ok = p.passwords[wildcard]
			if !ok {
				pwd = ""
			}
		}
		privateKey, cert, err = pkcs.Decode(pfxBytes, pwd)
	} else {
		err = invalidCertificatePath(path, err)
	}
	return
}

// EncryptColumnEncryptionKey encrypts a column encryption key using the column master key with the specified key path and using the specified algorithm.
func (p *Provider) EncryptColumnEncryptionKey(ctx context.Context, masterKeyPath string, encryptionAlgorithm string, cek []byte) (buf []byte, err error) {

	err = validateEncryptionAlgorithm(aecmk.Encryption, encryptionAlgorithm)
	if err != nil {
		return
	}
	err = validateKeyPathLength(aecmk.Encryption, masterKeyPath)
	if err != nil {
		return
	}
	pk, cert, err := p.tryLoadCertificate(aecmk.Encryption, masterKeyPath)
	if err != nil {
		return nil, err
	}
	publicKey := cert.PublicKey.(*rsa.PublicKey)
	keySizeInBytes := publicKey.Size()

	enc := unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewEncoder()
	// Start with version byte == 1
	tmp := []byte{byte(1)}
	// EncryptedColumnEncryptionKey = version + keyPathLength + ciphertextLength + keyPath + ciphertext +  signature
	// version
	keyPathBytes, err := enc.Bytes([]byte(strings.ToLower(masterKeyPath)))
	if err != nil {
		err = aecmk.NewError(aecmk.Encryption, "Unable to serialize key path", err)
		return
	}
	k := uint16(len(keyPathBytes))
	// keyPathLength
	tmp = append(tmp, byte(k), byte(k>>8))

	cipherText, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, publicKey, cek, []byte{})
	if err != nil {
		err = aecmk.NewError(aecmk.Encryption, "Unable to encrypt data", err)
		return
	}
	l := uint16(len(cipherText))
	// ciphertextLength
	tmp = append(tmp, byte(l), byte(l>>8))
	// keypath
	tmp = append(tmp, keyPathBytes...)
	// ciphertext
	tmp = append(tmp, cipherText...)
	hash := sha256.Sum256(tmp)
	// signature is the signed hash of the current buf
	sig, err := rsa.SignPKCS1v15(rand.Reader, pk.(*rsa.PrivateKey), crypto.SHA256, hash[:])
	if err != nil {
		err = aecmk.NewError(aecmk.Encryption, "Unable to sign encrypted data", err)
		return
	}
	if len(sig) != keySizeInBytes {
		err = aecmk.NewError(aecmk.Encryption, "Signature length doesn't match certificate key size", nil)
	} else {
		buf = append(tmp, sig...)
	}
	return
}

// SignColumnMasterKeyMetadata digitally signs the column master key metadata with the column master key
// referenced by the masterKeyPath parameter. The input values used to generate the signature should be the
// specified values of the masterKeyPath and allowEnclaveComputations parameters. May return an empty slice if not supported.
func (p *Provider) SignColumnMasterKeyMetadata(ctx context.Context, masterKeyPath string, allowEnclaveComputations bool) ([]byte, error) {
	return nil, nil
}

// VerifyColumnMasterKeyMetadata verifies the specified signature is valid for the column master key
// with the specified key path and the specified enclave behavior. Return nil if not supported.
func (p *Provider) VerifyColumnMasterKeyMetadata(ctx context.Context, masterKeyPath string, allowEnclaveComputations bool) (*bool, error) {
	return nil, nil
}

// KeyLifetime is an optional Duration. Keys fetched by this provider will be discarded after their lifetime expires.
// If it returns nil, the keys will expire based on the value of ColumnEncryptionKeyLifetime.
// If it returns zero, the keys will not be cached.
func (p *Provider) KeyLifetime() *time.Duration {
	return nil
}

func validateEncryptionAlgorithm(op aecmk.Operation, encryptionAlgorithm string) error {
	if !strings.EqualFold(encryptionAlgorithm, "RSA_OAEP") {
		return aecmk.NewError(op, fmt.Sprintf("Unsupported encryption algorithm %s", encryptionAlgorithm), nil)
	}
	return nil
}

func validateKeyPathLength(op aecmk.Operation, keyPath string) error {
	if len(keyPath) > 32767 {
		return aecmk.NewError(op, fmt.Sprintf("Key path is too long. %d", len(keyPath)), nil)
	}
	return nil
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
