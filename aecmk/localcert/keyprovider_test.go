//go:build go1.17
// +build go1.17

package localcert

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestThumbPrintToSignature(t *testing.T) {
	thumbprint := "5e89a107f0ade0aed5f753ecc60378b1bbae3598"
	signature := thumbprintToByteArray(thumbprint)
	if !bytes.Equal(signature, []byte{0x5e, 0x89, 0xa1, 0x07, 0xf0, 0xad, 0xe0, 0xae, 0xd5, 0xf7, 0x53, 0xec, 0xc6, 0x03, 0x78, 0xb1, 0xbb, 0xae, 0x35, 0x98}) {
		t.Fatalf("Incorrect signature bytes for %s. Got: %s", thumbprint, hex.Dump(signature))
	}
}
