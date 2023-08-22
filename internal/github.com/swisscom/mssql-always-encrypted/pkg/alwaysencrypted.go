package alwaysencrypted

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"unicode/utf16"
)

type CEKV struct {
	Version    int
	KeyPath    string
	Ciphertext []byte
	SignedHash []byte
	DataToSign []byte

	Key []byte
}

func (c *CEKV) VerifySignature(key *rsa.PublicKey) bool {
	sha256Sum := sha256.Sum256(c.DataToSign)
	err := rsa.VerifyPKCS1v15(key, crypto.SHA256, sha256Sum[:], c.SignedHash)

	return err == nil
}

func (c *CEKV) Verify(cert *x509.Certificate) bool {
	return c.VerifySignature(cert.PublicKey.(*rsa.PublicKey))
}

func (c *CEKV) Decrypt(private *rsa.PrivateKey) ([]byte, error) {
	decryptedData, decryptErr := rsa.DecryptOAEP(sha1.New(), rand.Reader, private, c.Ciphertext, nil)
	if decryptErr != nil {
		return nil, decryptErr
	}

	return decryptedData, nil
}

func LoadCEKV(bytes []byte) CEKV {
	idx := 0
	version := int(bytes[idx])
	idx++

	keyPathLengthBytes := bytes[idx : idx+2]
	keyPathLength := binary.LittleEndian.Uint16(keyPathLengthBytes)
	idx += 2

	cipherTextLengthBytes := bytes[idx : idx+2]
	cipherTextLength := binary.LittleEndian.Uint16(cipherTextLengthBytes)
	idx += 2

	keyPathBytes := bytes[idx : idx+int(keyPathLength)]
	idx += int(keyPathLength)

	var uint16Bytes []uint16
	for i := range keyPathBytes {
		if i%2 == 0 {
			continue
		}
		uint16Value := binary.LittleEndian.Uint16([]byte{keyPathBytes[i-1], keyPathBytes[i]})
		uint16Bytes = append(uint16Bytes, uint16Value)
	}
	keyPath := string(utf16.Decode(uint16Bytes))

	cipherText := bytes[idx : idx+int(cipherTextLength)]
	idx += int(cipherTextLength)

	dataToSign := bytes[0:idx]
	signedHash := bytes[idx:]

	return CEKV{
		Version:    version,
		KeyPath:    keyPath,
		DataToSign: dataToSign,
		Ciphertext: cipherText,
		SignedHash: signedHash,
	}
}
