package ntlm

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"
)

func TestLMOWFv1(t *testing.T) {
	hash := lmHash("Password")
	val := [21]byte{
		0xe5, 0x2c, 0xac, 0x67, 0x41, 0x9a, 0x9a, 0x22,
		0x4a, 0x3b, 0x10, 0x8f, 0x3f, 0xa6, 0xcb, 0x6d,
		0, 0, 0, 0, 0,
	}
	if hash != val {
		t.Errorf("got:\n%sexpected:\n%s", hex.Dump(hash[:]), hex.Dump(val[:]))
	}
}

func TestNTLMOWFv1(t *testing.T) {
	hash := ntlmHash("Password")
	val := [21]byte{
		0xa4, 0xf4, 0x9c, 0x40, 0x65, 0x10, 0xbd, 0xca, 0xb6, 0x82, 0x4e, 0xe7, 0xc3, 0x0f, 0xd8, 0x52,
		0, 0, 0, 0, 0,
	}
	if hash != val {
		t.Errorf("got:\n%sexpected:\n%s", hex.Dump(hash[:]), hex.Dump(val[:]))
	}
}

func TestNTLMv1Response(t *testing.T) {
	challenge := [8]byte{
		0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
	}
	nt := ntResponse(challenge, "Password")
	val := [24]byte{
		0x67, 0xc4, 0x30, 0x11, 0xf3, 0x02, 0x98, 0xa2, 0xad, 0x35, 0xec, 0xe6, 0x4f, 0x16, 0x33, 0x1c,
		0x44, 0xbd, 0xbe, 0xd9, 0x27, 0x84, 0x1f, 0x94,
	}
	if nt != val {
		t.Errorf("got:\n%sexpected:\n%s", hex.Dump(nt[:]), hex.Dump(val[:]))
	}
}

func TestLMv1Response(t *testing.T) {
	challenge := [8]byte{
		0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
	}
	nt := lmResponse(challenge, "Password")
	val := [24]byte{
		0x98, 0xde, 0xf7, 0xb8, 0x7f, 0x88, 0xaa, 0x5d, 0xaf, 0xe2, 0xdf, 0x77, 0x96, 0x88, 0xa1, 0x72,
		0xde, 0xf1, 0x1c, 0x7d, 0x5c, 0xcd, 0xef, 0x13,
	}
	if nt != val {
		t.Errorf("got:\n%sexpected:\n%s", hex.Dump(nt[:]), hex.Dump(val[:]))
	}
}

func TestNTLMSessionResponse(t *testing.T) {
	challenge := [8]byte{
		0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
	}
	nonce := [8]byte{
		0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
	}
	nt := ntlmSessionResponse(nonce, challenge, "Password")
	val := [24]byte{
		0x75, 0x37, 0xf8, 0x03, 0xae, 0x36, 0x71, 0x28, 0xca, 0x45, 0x82, 0x04, 0xbd, 0xe7, 0xca, 0xf8,
		0x1e, 0x97, 0xed, 0x26, 0x83, 0x26, 0x72, 0x32,
	}
	if nt != val {
		t.Errorf("got:\n%sexpected:\n%s", hex.Dump(nt[:]), hex.Dump(val[:]))
	}
}

func TestNTLMV2Response(t *testing.T) {
	target := "DOMAIN"
	username := "user"
	password := "SecREt01"
	challenge := [8]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
	targetInformationBlock, _ := hex.DecodeString("02000c0044004f004d00410049004e0001000c005300450052005600450052000400140064006f006d00610069006e002e0063006f006d00030022007300650072007600650072002e0064006f006d00610069006e002e0063006f006d0000000000")
	nonceBytes, _ := hex.DecodeString("ffffff0011223344")
	var nonce [8]byte
	copy(nonce[:8], nonceBytes[:])
	timestamp, err := time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
	if err != nil {
		panic(err)
	}

	expectedNTLMV2Response, _ := hex.DecodeString("5c788afec59c1fef3f90bf6ea419c02501010000000000000fc4a4d5fdf6b200ffffff00112233440000000002000c0044004f004d00410049004e0001000c005300450052005600450052000400140064006f006d00610069006e002e0063006f006d00030022007300650072007600650072002e0064006f006d00610069006e002e0063006f006d000000000000000000")
	expectedLMV2Response, _ := hex.DecodeString("d6e6152ea25d03b7c6ba6629c2d6aaf0ffffff0011223344")
	ntlmV2Response, lmV2Response := getNTLMv2AndLMv2ResponsePayloads(target, username, password, challenge, nonce, targetInformationBlock, timestamp)
	if !bytes.Equal(ntlmV2Response, expectedNTLMV2Response) {
		t.Errorf("got:\n%s\nexpected:\n%s", hex.Dump(ntlmV2Response), hex.Dump(expectedNTLMV2Response))
	}

	if !bytes.Equal(lmV2Response, expectedLMV2Response) {
		t.Errorf("got:\n%s\nexpected:\n%s", hex.Dump(ntlmV2Response), hex.Dump(expectedNTLMV2Response))
	}
}

func TestGetNTLMv2TargetInfoFields(t *testing.T) {
	type2Message, _ := hex.DecodeString("4e544c4d53535000020000000600060038000000058289026999bc21067c77f40000000000000000ac00ac003e0000000a0039380000000f4600570042000200060046005700420001000c00590037004100410041003400040022006000700065002e00610058006e0071006e0070006e00650074002e0063006f006d00030030007900370041004100410034002e006000700065002e00610058006e0071006e0070006e00650074002e0063006f006d00050024006100610058006d002e00610058006e0071006e0070006e00650074002e0063006f006d00070008007d9647e8aed6d50100000000")
	info, err := getNTLMv2TargetInfoFields(type2Message)
	if err != nil {
		t.Errorf("got:\n%e\nexpected:\nnil", err)
	}

	expectedResponseLength := 172
	responseLength := len(info)
	if responseLength != expectedResponseLength {
		t.Errorf("got:\n%d\nexpected:\n%d", responseLength, expectedResponseLength)
	}
}

func TestGetNTLMv2TargetInfoFieldsInvalidMessage(t *testing.T) {
	type2Message, _ := hex.DecodeString("4e544c4d53535000020000000600060038000000058289026999bc21067c77f40000000000000000ac00ac003e0000000a0039380000000f4600570042000200060046005700420001000c00590037004100410041003400040022006000700065002e00610058006e0071006e0070006e00650074002e0063006f006d00030030007900370041004100410034002e006000700065002e00610058006e0071006e0070006e00650074002e0063006f006d00050024006100610058006d002e00610058006e0071006e0070006e00650074002e0063006f006d00070008007")
	_, err := getNTLMv2TargetInfoFields(type2Message)
	if err == nil {
		t.Error("expected to get an error")
	}
}
