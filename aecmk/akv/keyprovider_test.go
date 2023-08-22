//go:build go1.18
// +build go1.18

package akv

import (
	"crypto/rand"
	"net/url"
	"testing"

	"github.com/microsoft/go-mssqldb/aecmk"
	"github.com/microsoft/go-mssqldb/internal/akvkeys"
	"github.com/stretchr/testify/assert"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	client, vaultURL, err := akvkeys.GetTestAKV()
	if err != nil {
		t.Skip("No access to AKV")
	}
	name, err := akvkeys.CreateRSAKey(client)
	assert.NoError(t, err, "CreateRSAKey")
	defer akvkeys.DeleteRSAKey(client, name)
	keyPath, _ := url.JoinPath(vaultURL, name)
	p := &KeyProvider
	plainKey := make([]byte, 32)
	_, _ = rand.Read(plainKey)
	t.Log("Plainkey:", plainKey)
	encryptedKey := p.EncryptColumnEncryptionKey(keyPath, aecmk.KeyEncryptionAlgorithm, plainKey)
	t.Log("Encryptedkey:", encryptedKey)
	assert.NotEqualValues(t, plainKey, encryptedKey, "encryptedKey is the same as plainKey")
	decryptedKey := p.DecryptColumnEncryptionKey(keyPath, aecmk.KeyEncryptionAlgorithm, encryptedKey)
	assert.Equalf(t, plainKey, decryptedKey, "decryptedkey doesn't match plainKey. %v : %v", decryptedKey, plainKey)
}
