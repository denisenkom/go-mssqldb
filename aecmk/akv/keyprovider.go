//go:build go1.18
// +build go1.18

package akv

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"math/big"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
	"github.com/microsoft/go-mssqldb/aecmk"
	ae "github.com/microsoft/go-mssqldb/internal/github.com/swisscom/mssql-always-encrypted/pkg"
	"golang.org/x/text/encoding/unicode"
)

const (
	wildcard = "*"
)

// Provider implements a column encryption key provider backed by Azure Key Vault
type Provider struct {
	// AllowedLocations constrains which locations the provider will use to find certificates. If empty, all locations are allowed.
	// When presented with a key store path whose endpoint not in the allowed list, the data will be returned still encrypted.
	AllowedLocations []string
	credentials      map[string]azcore.TokenCredential
}

type keyData struct {
	publicKey *rsa.PublicKey
	endpoint  string
	name      string
	version   string
}

// SetCertificateCredential stores the AzureCredential associated with the given AKV endpoint.
// If endpoint is empty the given credential applies to all endpoints that have not been explicitly assigned a value.
// If SetCertificateCredential is never called, the provider uses azidentity.DefaultAzureCredential.
func (p Provider) SetCertificateCredential(endpoint string, credential azcore.TokenCredential) {
	if endpoint == "" {
		endpoint = wildcard
	}
	p.credentials[endpoint] = credential
}

var KeyProvider = Provider{credentials: make(map[string]azcore.TokenCredential), AllowedLocations: make([]string, 0)}

func init() {
	err := aecmk.RegisterCekProvider(aecmk.AzureKeyVaultKeyProvider, &KeyProvider)
	if err != nil {
		panic(err)
	}
}

// DecryptColumnEncryptionKey decrypts the specified encrypted value of a column encryption key.
// The encrypted value is expected to be encrypted using the column master key with the specified key path and using the specified algorithm.
func (p *Provider) DecryptColumnEncryptionKey(masterKeyPath string, encryptionAlgorithm string, encryptedCek []byte) (decryptedKey []byte) {
	decryptedKey = nil
	keyData := p.getKeyData(masterKeyPath)
	if keyData == nil {
		return
	}
	keySize := keyData.publicKey.Size()
	cekv := ae.LoadCEKV(encryptedCek)
	if cekv.Version != 1 {
		panic(fmt.Errorf("Invalid version byte in encrypted key"))
	}
	if keySize != len(cekv.Ciphertext) {
		panic(fmt.Errorf("Encrypted key has wrong ciphertext length"))
	}
	if keySize != len(cekv.SignedHash) {
		panic(fmt.Errorf("Encrypted key signature length mismatch"))
	}
	if !cekv.VerifySignature(keyData.publicKey) {
		panic(fmt.Errorf("Invalid signature hash"))
	}

	client := p.getAKVClient(keyData.endpoint)
	algorithm := getAlgorithm(encryptionAlgorithm)
	parameters := azkeys.KeyOperationParameters{
		Algorithm: &algorithm,
		Value:     cekv.Ciphertext,
	}
	r, err := client.UnwrapKey(context.Background(), keyData.name, keyData.version, parameters, nil)
	if err != nil {
		panic(fmt.Errorf("Unable to decrypt key %s: %w", masterKeyPath, err))
	}
	decryptedKey = r.Result
	return
}

// EncryptColumnEncryptionKey encrypts a column encryption key using the column master key with the specified key path and using the specified algorithm.
func (p *Provider) EncryptColumnEncryptionKey(masterKeyPath string, encryptionAlgorithm string, cek []byte) []byte {
	keyData := p.getKeyData(masterKeyPath)
	// just validate the algorith
	_ = getAlgorithm(encryptionAlgorithm)
	keySize := keyData.publicKey.Size()
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

	cipherText, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, keyData.publicKey, cek, []byte{})
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
	client := p.getAKVClient(keyData.endpoint)
	signAlgorithm := azkeys.SignatureAlgorithmRS256
	parameters := azkeys.SignParameters{
		Algorithm: &signAlgorithm,
		Value:     hash[:],
	}
	r, err := client.Sign(context.Background(), keyData.name, keyData.version, parameters, nil)
	if err != nil {
		panic(err)
	}
	if len(r.Result) != keySize {
		panic("Signature length doesn't match certificate key size")
	}
	// signature
	buf = append(buf, r.Result...)
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

func getAlgorithm(encryptionAlgorithm string) (algorithm azkeys.EncryptionAlgorithm) {
	// support both RSA_OAEP and RSA-OAEP
	if strings.EqualFold(encryptionAlgorithm, aecmk.KeyEncryptionAlgorithm) {
		encryptionAlgorithm = string(azkeys.EncryptionAlgorithmRSAOAEP)
	}
	if !strings.EqualFold(encryptionAlgorithm, string(azkeys.EncryptionAlgorithmRSAOAEP)) {
		panic(fmt.Errorf("Unsupported encryption algorithm %s", encryptionAlgorithm))
	}
	return azkeys.EncryptionAlgorithmRSAOAEP
}

// masterKeyPath is a full URL. The AKV client requires it broken down into endpoint, name, and version
// The URL has format '{endpoint}/{host}/keys/{name}/[{version}/]'
func (p *Provider) getKeyData(masterKeyPath string) *keyData {
	endpoint, keypath, allowed := p.allowedPathAndEndpoint(masterKeyPath)
	if !(allowed) {
		return nil
	}
	k := &keyData{
		endpoint: endpoint,
		name:     keypath[0],
	}
	if len(keypath) > 1 {
		k.version = keypath[1]
	}
	client := p.getAKVClient(endpoint)
	r, err := client.GetKey(context.Background(), k.name, k.version, nil)
	if err != nil {
		panic(fmt.Errorf("Unable to get key from AKV %w", err))
	}
	if r.Key.Kty == nil || (*r.Key.Kty != azkeys.KeyTypeRSA && *r.Key.Kty != azkeys.KeyTypeRSAHSM) {
		panic(fmt.Errorf("Key type not supported for Always Encrypted"))
	}
	k.publicKey = &rsa.PublicKey{
		N: new(big.Int).SetBytes(r.Key.N),
		E: int(new(big.Int).SetBytes(r.Key.E).Int64()),
	}
	return k
}

func (p *Provider) allowedPathAndEndpoint(masterKeyPath string) (endpoint string, keypath []string, allowed bool) {
	allowed = len(p.AllowedLocations) == 0
	url, err := url.Parse(masterKeyPath)
	if err != nil {
		panic(fmt.Errorf("Invalid URL for master key path %s: %w", masterKeyPath, err))
	}
	if !allowed {

	loop:
		for _, l := range p.AllowedLocations {
			if strings.HasSuffix(strings.ToLower(url.Host), strings.ToLower(l)) {
				allowed = true
				break loop
			}
		}
	}
	if allowed {
		pathParts := strings.Split(strings.TrimLeft(url.Path, "/"), "/")
		if len(pathParts) < 2 || len(pathParts) > 3 || pathParts[0] != "keys" {
			panic(fmt.Errorf("Invalid URL for master key path %s", masterKeyPath))
		}
		keypath = pathParts[1:]
		url.Path = ""
		url.RawQuery = ""
		url.Fragment = ""
		endpoint = url.String()
	}
	return
}

func (p *Provider) getAKVClient(endpoint string) (client *azkeys.Client) {
	client, err := azkeys.NewClient(endpoint, p.getCredential(endpoint), nil)
	if err != nil {
		panic(fmt.Errorf("Unable to create AKV client %w", err))
	}
	return
}

func (p *Provider) getCredential(endpoint string) azcore.TokenCredential {
	if len(p.credentials) == 0 {
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			panic(fmt.Errorf("Unable to create a default credential: %w", err))
		}
		p.credentials[wildcard] = credential
		return credential
	}
	if credential, ok := p.credentials[endpoint]; ok {
		return credential
	}
	if credential, ok := p.credentials[wildcard]; ok {
		return credential
	}
	panic(fmt.Errorf("No credential available for AKV path %s", endpoint))
}
