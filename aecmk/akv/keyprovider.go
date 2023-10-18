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
func (p *Provider) DecryptColumnEncryptionKey(ctx context.Context, masterKeyPath string, encryptionAlgorithm string, encryptedCek []byte) (decryptedKey []byte, err error) {
	decryptedKey = nil
	keyData, err := p.getKeyData(ctx, masterKeyPath, aecmk.Decryption)
	if err != nil {
		return
	}
	keySize := keyData.publicKey.Size()
	cekv := ae.LoadCEKV(encryptedCek)
	if cekv.Version != 1 {
		return nil, aecmk.NewError(aecmk.Decryption, "Invalid version byte in encrypted key", nil)
	}
	if keySize != len(cekv.Ciphertext) {
		return nil, aecmk.NewError(aecmk.Decryption, "Encrypted key has wrong ciphertext length", nil)
	}
	if keySize != len(cekv.SignedHash) {
		return nil, aecmk.NewError(aecmk.Decryption, "Encrypted key signature length mismatch", nil)
	}
	if !cekv.VerifySignature(keyData.publicKey) {
		return nil, aecmk.NewError(aecmk.Decryption, "Invalid signature hash", nil)
	}

	client, err := p.getAKVClient(aecmk.Decryption, keyData.endpoint)
	if err != nil {
		return
	}
	algorithm, err := getAlgorithm(aecmk.Decryption, encryptionAlgorithm)
	if err != nil {
		return
	}
	parameters := azkeys.KeyOperationParameters{
		Algorithm: &algorithm,
		Value:     cekv.Ciphertext,
	}
	r, e := client.UnwrapKey(ctx, keyData.name, keyData.version, parameters, nil)
	if e != nil {
		err = aecmk.NewError(aecmk.Decryption, fmt.Sprintf("Unable to decrypt key %s", masterKeyPath), e)
	} else {
		decryptedKey = r.Result
	}
	return
}

// EncryptColumnEncryptionKey encrypts a column encryption key using the column master key with the specified key path and using the specified algorithm.
func (p *Provider) EncryptColumnEncryptionKey(ctx context.Context, masterKeyPath string, encryptionAlgorithm string, cek []byte) (buf []byte, err error) {
	keyData, err := p.getKeyData(ctx, masterKeyPath, aecmk.Encryption)
	if err != nil {
		return
	}
	_, err = getAlgorithm(aecmk.Encryption, encryptionAlgorithm)
	if err != nil {
		return
	}
	keySize := keyData.publicKey.Size()
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

	cipherText, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, keyData.publicKey, cek, []byte{})
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
	client, err := p.getAKVClient(aecmk.Encryption, keyData.endpoint)
	if err != nil {
		return
	}
	signAlgorithm := azkeys.SignatureAlgorithmRS256
	parameters := azkeys.SignParameters{
		Algorithm: &signAlgorithm,
		Value:     hash[:],
	}
	r, err := client.Sign(ctx, keyData.name, keyData.version, parameters, nil)
	if err != nil {
		err = aecmk.NewError(aecmk.Encryption, "AKV failed to sign data", err)
		return
	}
	if len(r.Result) != keySize {
		err = aecmk.NewError(aecmk.Encryption, "Signature length doesn't match certificate key size", nil)
	} else {
		// signature
		buf = append(tmp, r.Result...)
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

func getAlgorithm(op aecmk.Operation, encryptionAlgorithm string) (algorithm azkeys.EncryptionAlgorithm, err error) {
	// support both RSA_OAEP and RSA-OAEP
	if strings.EqualFold(encryptionAlgorithm, aecmk.KeyEncryptionAlgorithm) {
		encryptionAlgorithm = string(azkeys.EncryptionAlgorithmRSAOAEP)
	}
	if !strings.EqualFold(encryptionAlgorithm, string(azkeys.EncryptionAlgorithmRSAOAEP)) {
		err = aecmk.NewError(op, fmt.Sprintf("Unsupported encryption algorithm %s", encryptionAlgorithm), nil)
	} else {
		algorithm = azkeys.EncryptionAlgorithmRSAOAEP
	}
	return
}

// masterKeyPath is a full URL. The AKV client requires it broken down into endpoint, name, and version
// The URL has format '{endpoint}/{host}/keys/{name}/[{version}/]'
func (p *Provider) getKeyData(ctx context.Context, masterKeyPath string, op aecmk.Operation) (k *keyData, err error) {
	endpoint, keypath, allowed := p.allowedPathAndEndpoint(masterKeyPath)
	if !(allowed) {
		err = aecmk.KeyPathNotAllowed(masterKeyPath, op)
		return
	}
	k = &keyData{
		endpoint: endpoint,
		name:     keypath[0],
	}
	if len(keypath) > 1 {
		k.version = keypath[1]
	}
	client, err := p.getAKVClient(op, endpoint)
	if err != nil {
		return
	}
	r, err := client.GetKey(ctx, k.name, k.version, nil)
	if err != nil {
		err = aecmk.NewError(op, "Unable to get key from AKV. Name:"+masterKeyPath, err)
	}
	if r.Key.Kty == nil || (*r.Key.Kty != azkeys.KeyTypeRSA && *r.Key.Kty != azkeys.KeyTypeRSAHSM) {
		err = aecmk.NewError(op, "Key type not supported for Always Encrypted", nil)
	}
	if err == nil {
		k.publicKey = &rsa.PublicKey{
			N: new(big.Int).SetBytes(r.Key.N),
			E: int(new(big.Int).SetBytes(r.Key.E).Int64()),
		}
	}
	return
}

func (p *Provider) allowedPathAndEndpoint(masterKeyPath string) (endpoint string, keypath []string, allowed bool) {
	allowed = len(p.AllowedLocations) == 0
	url, err := url.Parse(masterKeyPath)
	if err != nil {
		allowed = false
		return
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
			allowed = false
			return
		}
		keypath = pathParts[1:]
		url.Path = ""
		url.RawQuery = ""
		url.Fragment = ""
		endpoint = url.String()
	}
	return
}

func (p *Provider) getAKVClient(op aecmk.Operation, endpoint string) (client *azkeys.Client, err error) {
	credential, err := p.getCredential(op, endpoint)
	if err == nil {
		client, err = azkeys.NewClient(endpoint, credential, nil)
	}
	if err != nil {
		err = aecmk.NewError(op, "Unable to create AKV client", err)
	}
	return
}

func (p *Provider) getCredential(op aecmk.Operation, endpoint string) (credential azcore.TokenCredential, err error) {
	if len(p.credentials) == 0 {
		credential, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			err = aecmk.NewError(op, "Unable to create a default credential", err)
		} else {
			p.credentials[wildcard] = credential
		}
		return
	}
	var ok bool
	if credential, ok = p.credentials[endpoint]; ok {
		return
	}
	if credential, ok = p.credentials[wildcard]; ok {
		return
	}
	err = aecmk.NewError(op, fmt.Sprintf("No credential available for AKV path %s", endpoint), nil)
	return
}
