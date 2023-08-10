package alwaysencrypted

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/microsoft/go-mssqldb/internal/github.com/swisscom/mssql-always-encrypted/pkg/algorithms"
	"github.com/microsoft/go-mssqldb/internal/github.com/swisscom/mssql-always-encrypted/pkg/encryption"
	"github.com/microsoft/go-mssqldb/internal/github.com/swisscom/mssql-always-encrypted/pkg/keys"
	"github.com/stretchr/testify/assert"
	"golang.org/x/text/encoding/unicode"
)

func TestLoadCEKV(t *testing.T) {
	certFile, err := os.Open("../test/always-encrypted_pub.pem")
	assert.NoError(t, err)

	certBytes, err := ioutil.ReadAll(certFile)
	assert.NoError(t, err)
	pemB, _ := pem.Decode(certBytes)
	cert, err := x509.ParseCertificate(pemB.Bytes)
	assert.NoError(t, err)

	cekvFile, err := os.Open("../test/cekv.key")
	assert.NoError(t, err)
	cekvBytes, err := ioutil.ReadAll(cekvFile)
	assert.NoError(t, err)
	cekv := LoadCEKV(cekvBytes)
	assert.Equal(t, 1, cekv.Version)
	assert.True(t, cekv.Verify(cert))
}
func TestDecrypt(t *testing.T) {
	certFile, err := os.Open("../test/always-encrypted.pem")
	assert.NoError(t, err)

	certBytes, err := ioutil.ReadAll(certFile)
	assert.NoError(t, err)
	pemB, _ := pem.Decode(certBytes)
	privKey, err := x509.ParsePKCS8PrivateKey(pemB.Bytes)
	assert.NoError(t, err)

	rsaPrivKey := privKey.(*rsa.PrivateKey)

	cekvFile, err := os.Open("../test/cekv.key")
	assert.NoError(t, err)
	cekvBytes, err := ioutil.ReadAll(cekvFile)
	assert.NoError(t, err)
	cekv := LoadCEKV(cekvBytes)
	rootKey, err := cekv.Decrypt(rsaPrivKey)
	assert.NoError(t, err)
	assert.Equal(t, "0ff9e45335df3dec7be0649f741e6ea870e9d49d16fe4be7437ce22489f48ead", fmt.Sprintf("%02x", rootKey))
	assert.Equal(t, 1, cekv.Version)
	assert.NotNil(t, rootKey)

	columnBytesFile, err := os.Open("../test/column_value.enc")
	assert.NoError(t, err)

	columnBytes, err := ioutil.ReadAll(columnBytesFile)
	assert.NoError(t, err)

	key := keys.NewAeadAes256CbcHmac256(rootKey)
	alg := algorithms.NewAeadAes256CbcHmac256Algorithm(key, encryption.Deterministic, 1)
	cleartext, err := alg.Decrypt(columnBytes)
	assert.NoErrorf(t, err, "Decrypt failed! %v", err)

	enc := unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM)
	decoder := enc.NewDecoder()
	cleartextUtf8, err := decoder.Bytes(cleartext)
	assert.NoError(t, err)
	t.Logf("column value: \"%02X\"", cleartextUtf8)
	assert.Equal(t, "12345     ", string(cleartextUtf8))
}
func TestDecryptCEK(t *testing.T) {
	certFile, err := os.Open("../test/always-encrypted.pem")
	assert.NoError(t, err)

	certFileBytes, err := ioutil.ReadAll(certFile)
	assert.NoError(t, err)

	pemBlock, _ := pem.Decode(certFileBytes)
	cert, err := x509.ParsePKCS8PrivateKey(pemBlock.Bytes)
	assert.NoError(t, err)

	cekvFile, err := os.Open("../test/cekv.key")
	assert.NoError(t, err)

	cekvBytes, err := ioutil.ReadAll(cekvFile)
	assert.NoError(t, err)

	cekv := LoadCEKV(cekvBytes)
	t.Logf("Cert: %v\n", cert)

	rsaKey := cert.(*rsa.PrivateKey)

	// RSA/ECB/OAEPWithSHA-1AndMGF1Padding
	bytes, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, rsaKey, cekv.Ciphertext, nil)
	assert.NoError(t, err)
	t.Logf("Key: %02x\n", bytes)
}
