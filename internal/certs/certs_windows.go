//go:build go1.17
// +build go1.17

package certs

import (
	"crypto/rsa"
	"crypto/x509"
	"errors"
	"fmt"
	"math/big"

	"unsafe"

	"golang.org/x/sys/windows"
)

func FindCertBySignatureHash(storeHandle windows.Handle, hash []byte) (pk interface{}, cert *x509.Certificate, err error) {
	var certContext *windows.CertContext
	cryptoAPIBlob := windows.CryptHashBlob{
		Size: uint32(len(hash)),
		Data: &hash[0],
	}

	certContext, err = windows.CertFindCertificateInStore(
		storeHandle,
		windows.X509_ASN_ENCODING|windows.PKCS_7_ASN_ENCODING,
		0,
		windows.CERT_FIND_HASH,
		unsafe.Pointer(&cryptoAPIBlob),
		nil)

	if err != nil {
		err = fmt.Errorf("Unable to find certificate by signature hash. %w", err)
		return
	}
	pk, cert, err = certContextToX509(certContext)
	return
}

func certContextToX509(ctx *windows.CertContext) (pk interface{}, cert *x509.Certificate, err error) {
	// To ensure we don't mess with the cert context's memory, use a copy of it.
	src := (*[1 << 20]byte)(unsafe.Pointer(ctx.EncodedCert))[:ctx.Length:ctx.Length]
	der := make([]byte, int(ctx.Length))
	copy(der, src)

	cert, err = x509.ParseCertificate(der)
	if err != nil {
		return
	}
	var kh windows.Handle
	var keySpec uint32
	var freeProvOrKey bool
	err = windows.CryptAcquireCertificatePrivateKey(ctx, windows.CRYPT_ACQUIRE_ONLY_NCRYPT_KEY_FLAG, nil, &kh, &keySpec, &freeProvOrKey)
	if err != nil {
		return
	}

	pkBytes, err := nCryptExportKey(kh, "RSAFULLPRIVATEBLOB")
	if freeProvOrKey {
		_, _, _ = procNCryptFreeObject.Call(uintptr(kh))
	}
	if err != nil {
		return
	}

	pk, err = unmarshalRSA(pkBytes)
	return
}

var (
	nCrypt               = windows.MustLoadDLL("ncrypt.dll")
	procNCryptExportKey  = nCrypt.MustFindProc("NCryptExportKey")
	procNCryptFreeObject = nCrypt.MustFindProc("NCryptFreeObject")
)

// wide returns a pointer to a uint16 representing the equivalent
// to a Windows LPCWSTR.
func wide(s string) *uint16 {
	w, _ := windows.UTF16PtrFromString(s)
	return w
}

func nCryptExportKey(kh windows.Handle, blobType string) ([]byte, error) {
	var size uint32
	// When obtaining the size of a public key, most parameters are not required
	r, _, err := procNCryptExportKey.Call(
		uintptr(kh),
		0,
		uintptr(unsafe.Pointer(wide(blobType))),
		0,
		0,
		0,
		uintptr(unsafe.Pointer(&size)),
		0)
	if !errors.Is(err, windows.Errno(0)) {
		return nil, fmt.Errorf("nCryptExportKey returned %w", err)
	}
	if r != 0 {
		return nil, fmt.Errorf("NCryptExportKey returned 0x%X during size check", uint32(r))
	}

	// Place the exported key in buf now that we know the size required
	buf := make([]byte, size)
	r, _, err = procNCryptExportKey.Call(
		uintptr(kh),
		0,
		uintptr(unsafe.Pointer(wide(blobType))),
		0,
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(size),
		uintptr(unsafe.Pointer(&size)),
		0)
	if !errors.Is(err, windows.Errno(0)) {
		return nil, fmt.Errorf("nCryptExportKey returned %w", err)
	}
	if r != 0 {
		return nil, fmt.Errorf("NCryptExportKey returned 0x%X during export", uint32(r))
	}
	return buf, nil
}

type RSA_HEADER struct {
	Magic         uint32
	BitLength     uint32
	PublicExpSize uint32
	ModulusSize   uint32
	Prime1Size    uint32
	Prime2Size    uint32
}

func unmarshalRSA(buf []byte) (*rsa.PrivateKey, error) {
	// BCRYPT_RSA_BLOB -- https://learn.microsoft.com/windows/win32/api/bcrypt/ns-bcrypt-bcrypt_rsakey_blob
	cbHeader := uint32(unsafe.Sizeof(RSA_HEADER{}))
	header := (*(*RSA_HEADER)(unsafe.Pointer(&buf[0])))
	buf = buf[cbHeader:]
	if header.Magic != 0x33415352 { // "RSA3" BCRYPT_RSAFULLPRIVATE_MAGIC
		return nil, fmt.Errorf("invalid header magic %x", header.Magic)
	}

	if header.PublicExpSize > 8 {
		return nil, fmt.Errorf("unsupported public exponent size (%d bits)", header.PublicExpSize*8)
	}

	consumeBigInt := func(size uint32) *big.Int {
		b := buf[:size]
		buf = buf[size:]
		return new(big.Int).SetBytes(b)
	}
	E := consumeBigInt(header.PublicExpSize)
	N := consumeBigInt(header.ModulusSize)
	P := consumeBigInt(header.Prime1Size)
	Q := consumeBigInt(header.Prime2Size)
	Dp := consumeBigInt(header.Prime1Size)
	Dq := consumeBigInt(header.Prime2Size)
	Qinv := consumeBigInt(header.Prime1Size)
	D := consumeBigInt(header.ModulusSize)

	pk := &rsa.PrivateKey{
		PublicKey: rsa.PublicKey{
			N: N,
			E: int(E.Int64()),
		},
		D:      D,
		Primes: []*big.Int{P, Q},
		Precomputed: rsa.PrecomputedValues{Dp: Dp,
			Dq: Dq, Qinv: Qinv,
		},
	}
	return pk, nil
}
