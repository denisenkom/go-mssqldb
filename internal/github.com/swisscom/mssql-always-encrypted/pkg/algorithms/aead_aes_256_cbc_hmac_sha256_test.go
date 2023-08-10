package algorithms_test

import (
	"encoding/hex"
	"testing"

	"github.com/microsoft/go-mssqldb/internal/github.com/swisscom/mssql-always-encrypted/pkg/algorithms"
	"github.com/microsoft/go-mssqldb/internal/github.com/swisscom/mssql-always-encrypted/pkg/encryption"
	"github.com/microsoft/go-mssqldb/internal/github.com/swisscom/mssql-always-encrypted/pkg/keys"
	"github.com/stretchr/testify/assert"
)

func TestAeadAes256CbcHmac256Algorithm_Decrypt(t *testing.T) {
	expectedResult, err := hex.DecodeString("3100320033003400350020002000200020002000")
	if err != nil {
		t.Fatal(err)
	}

	cipherText, err := hex.DecodeString("0181c4b77e1c50583c5e83a20afd4c98ce5acb39a636f00247b3a4d78a8be319c840e6970541a66723583def227eb774b4234cff209443b0209b75309532b527bdf9b2dfb326b4428840532a20460d06d4")
	if err != nil {
		t.Fatal(err)
	}

	rootKey, err := hex.DecodeString("0ff9e45335df3dec7be0649f741e6ea870e9d49d16fe4be7437ce22489f48ead")
	if err != nil {
		t.Fatal(err)
	}

	key := keys.NewAeadAes256CbcHmac256(rootKey)
	alg := algorithms.NewAeadAes256CbcHmac256Algorithm(key, encryption.Deterministic, 1)

	result, err := alg.Decrypt(cipherText)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expectedResult, result)
}
