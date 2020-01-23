package mssql

import (
	"reflect"
	"testing"
)

func Test_parseFedAuthInfo(t *testing.T) {
	// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/0e4486d6-d407-4962-9803-0c1a4d4d87ce
	tokenBytes := []byte{
		4 + 9 + 9 + 58 + 68, 0, 0, 0, // TokenLength
		2, 0, 0, 0, // CountOfInfoIDs
		// FedAuthInfoOpts:
		2,           //  FedAuthInfoID = SPN
		58, 0, 0, 0, //  FedAuthInfoDataLen
		4 + 18, 0, 0, 0, //  FedAuthInfoDataOffset
		1,           //  FedAuthInfoID = STSURL
		68, 0, 0, 0, //  FedAuthInfoDataLen
		4 + 18 + 58, 0, 0, 0, //  FedAuthInfoDataOffset

		// https://database.windows.net/
		// 58 bytes
		104, 0, 116, 0, 116, 0, 112, 0, 115, 0, 58, 0, 47, 0, 47, 0, 100, 0, 97, 0, 116, 0, 97, 0, 98, 0, 97, 0, 115, 0, 101, 0, 46, 0, 119, 0, 105, 0, 110, 0, 100, 0, 111, 0, 119, 0, 115, 0, 46, 0, 110, 0, 101, 0, 116, 0, 47, 0,
		// https://login.microsoftonline.com/
		// 68 bytes
		104, 0, 116, 0, 116, 0, 112, 0, 115, 0, 58, 0, 47, 0, 47, 0, 108, 0, 111, 0, 103, 0, 105, 0, 110, 0, 46, 0, 109, 0, 105, 0, 99, 0, 114, 0, 111, 0, 115, 0, 111, 0, 102, 0, 116, 0, 111, 0, 110, 0, 108, 0, 105, 0, 110, 0, 101, 0, 46, 0, 99, 0, 111, 0, 109, 0, 47, 0,
	}

	memBuf := new(MockTransport)
	buf := newTdsBuffer(1024, memBuf)
	buf.rbuf = tokenBytes
	buf.rsize = len(tokenBytes)
	buf.rpos = 0

	got := parseFedAuthInfo(buf)
	want := fedAuthInfoStruct{
		STSURL:    "https://login.microsoftonline.com/",
		ServerSPN: "https://database.windows.net/",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Expected %+v, got %+v", want, got)
	}
}

func Test_parseFeatureExtAck(t *testing.T) {
	// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/2eb82f8e-11f0-46dc-b42d-27302fa4701a

	testCases := []struct {
		name       string
		tokenBytes []byte
		expected   map[byte]interface{}
	}{
		{"Nonce and Signature",
			[]byte{
				0x02,        // FeatureId == FEDAUTH
				64, 0, 0, 0, // FeatureAckDataLen
				// nonce
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
				// sig
				31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0,

				0xff, // terminator
			},
			map[byte]interface{}{
				2: fedAuthAckStruct{
					Nonce:     []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
					Signature: []byte{31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
				}},
		},
		{
			"Nonce only",
			[]byte{
				0x02,        // FeatureId == FEDAUTH
				32, 0, 0, 0, // FeatureAckDataLen
				// nonce
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
				0xff, // terminator
			},
			map[byte]interface{}{
				2: fedAuthAckStruct{
					Nonce: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
				}},
		},
		{
			"Empty",
			[]byte{
				0x02,       // FeatureId == FEDAUTH
				0, 0, 0, 0, // FeatureAckDataLen
				0xff, // terminator
			},
			map[byte]interface{}{
				2: fedAuthAckStruct{}},
		},
		{
			"Ignored",
			[]byte{
				// this feature should be ignored, go-mssqldb does not handle it
				0x08,       // FeatureId == AZURESQLSUPPORT
				1, 0, 0, 0, // FeatureAckDataLen
				0x0, //The server does not support the AZURESQLSUPPORT feature extension.

				0x02,       // FeatureId == FEDAUTH
				0, 0, 0, 0, // FeatureAckDataLen
				0xff, // terminator
			},
			map[byte]interface{}{
				2: fedAuthAckStruct{}},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			memBuf := new(MockTransport)
			buf := newTdsBuffer(1024, memBuf)
			buf.rbuf = tt.tokenBytes
			buf.rsize = len(tt.tokenBytes)
			buf.rpos = 0

			got := parseFeatureExtAck(buf)
			want := tt.expected

			if !reflect.DeepEqual(got, want) {
				t.Fatalf("Expected %+v, got %+v", want, got)
			}
		})
	}
}
