package mssql

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"sync/atomic"
	"testing"

	"github.com/microsoft/go-mssqldb/msdsn"
)

type MockTransportDialer struct {
	expected, responses []string
	server, client      net.Conn
	result              chan error
	count               int32
}

func NewMockTransportDialer(expected, responses []string) *MockTransportDialer {
	server, client := net.Pipe()

	return &MockTransportDialer{
		expected:  expected,
		responses: responses,
		server:    server,
		client:    client,
		result:    make(chan error, 2),
	}
}

func (d *MockTransportDialer) DialContext(ctx context.Context, network string, addr string) (conn net.Conn, err error) {
	if atomic.AddInt32(&d.count, 1) != 1 {
		return nil, errors.New("No concurrent connections to mock dialer")
	}

	go testLoginSequenceServer(d.result, d.server, d.expected, d.responses)

	return d.client, nil
}

func testLoginSequenceServer(result chan error, conn net.Conn, expectedPackets, responsePackets []string) {
	defer func() {
		conn.Close()
		close(result)
	}()

	spacesRE := regexp.MustCompile(`\s+`)

	packet := make([]byte, 1024)
	for i, expectedHex := range expectedPackets {
		expectedBytes, err := hex.DecodeString(spacesRE.ReplaceAllString(expectedHex, ""))
		if err != nil {
			result <- err
			return
		}

		for b := 0; b < len(expectedBytes) && err == nil; {
			n, err := conn.Read(packet)

			// Ignore EOF: ErrPipeClosed is the real signal
			if err == io.EOF {
				err = nil
				continue
			}

			if err != nil {
				result <- err
				return
			}

			for bi := 0; bi < n; bi++ {
				if expectedBytes[bi+b] != packet[bi] {
					suffix := ""
					if bi > 0 {
						suffix = fmt.Sprintf("Previous byte: %02X", packet[bi-1])
					}
					if bi < n {
						suffix = fmt.Sprintf("%s Next byte:%02X", suffix, packet[bi+1])
					}
					err = fmt.Errorf("Client sent unexpected byte %02X != %02X at offset %d of packet %d. %s",
						packet[bi], expectedBytes[bi+b], bi+b, i, suffix)

					result <- err
					return
				}
			}

			b = b + n
		}

		if i >= len(responsePackets) || responsePackets[i] == "" {
			continue
		}

		responseBytes, err := hex.DecodeString(spacesRE.ReplaceAllString(responsePackets[i], ""))
		if err != nil {
			result <- err
			return
		}

		for b := 0; b < len(responseBytes); {
			n, err := conn.Write(responseBytes[b:])

			if err != nil {
				result <- err
				return
			}

			b = b + n
		}
	}

	result <- nil
}

func TestLoginWithSQLServerAuth(t *testing.T) {
	conn, err := NewConnector("sqlserver://test:secret@localhost:1433?Workstation ID=localhost&log=128")
	if err != nil {
		t.Errorf("Unable to parse dummy DSN: %v", err)
	}
	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)

	mock := NewMockTransportDialer(
		[]string{
			"  12 01 00 2f 00 00 01 00  00 00 1a 00 06 01 00 20\n" +
				"00 01 02 00 21 00 01 03  00 22 00 04 04 00 26 00\n" +
				"01 ff 00 00 00 00 00 00  00 00 00 00 00 00 00\n",
			"  10 01 00 b2 00 00 01 00  aa 00 00 00 04 00 00 74\n" +
				"00 10 00 00 00 00 00 00  00 00 00 00 00 00 00 00\n" +
				"A0 02 00 00 00 00 00 00  00 00 00 00 5e 00 09 00\n" +
				"70 00 04 00 78 00 06 00  84 00 0a 00 98 00 09 00\n" +
				"00 00 00 00 aa 00 00 00  aa 00 00 00 aa 00 00 00\n" +
				"00 00 00 00 00 00 aa 00  00 00 aa 00 00 00 aa 00\n" +
				"00 00 00 00 00 00 6c 00  6f 00 63 00 61 00 6c 00\n" +
				"68 00 6f 00 73 00 74 00  74 00 65 00 73 00 74 00\n" +
				"92 a5 f3 a5 93 a5 82 a5  f3 a5 e2 a5 67 00 6f 00\n" +
				"2d 00 6d 00 73 00 73 00  71 00 6c 00 64 00 62 00\n" +
				"6c 00 6f 00 63 00 61 00  6c 00 68 00 6f 00 73 00\n" +
				"74 00\n",
		},
		[]string{
			"  04 01 00 20  00 00 01 00   00 00 10 00  06 01 00 16\n" +
				"00 01 06 00  17 00 01 FF   0C 00 07 D0  00 00 02 01\n",
			"  04 01 00 4A  00 00 01 00   AD 32 00 01 74  00 00 04\n" +
				"14 4d 00 69  00 63 00 72   00 6f 00 73  00 6f 00 66\n" +
				"00 74 00 20  00 53 00 51   00 4c 00 20  00 53 00 65\n" +
				"00 72 00 76  00 65 00 72   00 0c 00 07  d0 fd 00 00\n" +
				"00 00 00 00  00 00 00 00   00 00\n",
		},
	)

	conn.Dialer = mock

	_, err = connect(context.Background(), conn, driverInstanceNoProcess.logger, conn.params)
	if err != nil {
		t.Error(err)
	}

	err = <-mock.result
	if err != nil {
		t.Error(err)
	}
}

func TestLoginWithSecurityTokenAuth(t *testing.T) {
	config, err := msdsn.Parse("sqlserver://localhost:1433?Workstation ID=localhost&log=128")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := NewSecurityTokenConnector(config,
		func(ctx context.Context) (string, error) {
			return "<token>", nil
		},
	)
	if err != nil {
		t.Errorf("Unable to parse dummy DSN: %v", err)
	}

	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)

	mock := NewMockTransportDialer(
		[]string{
			"  12 01 00 35 00 00 01 00  00 00 1F 00 06 01 00 25\n" +
				"00 01 02 00 26 00 01 03  00 27 00 04 04 00 2B 00\n" +
				"01 06 00 2c 00 01 ff 00  00 00 00 00 00 00 00 00\n" +
				"00 00 00 00 01\n",
			"  10 01 00 BB 00 00 01 00  B3 00 00 00 04 00 00 74\n" +
				"00 10 00 00 00 00 00 00  00 00 00 00 00 00 00 00\n" +
				"A0 02 00 10 00 00 00 00  00 00 00 00 5E 00 09 00\n" +
				"70 00 00 00 70 00 00 00  70 00 0A 00 84 00 09 00\n" +
				"96 00 04 00 96 00 00 00  96 00 00 00 96 00 00 00\n" +
				"00 00 00 00 00 00 96 00  00 00 96 00 00 00 96 00\n" +
				"00 00 00 00 00 00 6C 00  6F 00 63 00 61 00 6C 00\n" +
				"68 00 6F 00 73 00 74 00  67 00 6F 00 2D 00 6D 00\n" +
				"73 00 73 00 71 00 6C 00  64 00 62 00 6C 00 6F 00\n" +
				"63 00 61 00 6C 00 68 00  6F 00 73 00 74 00 9A 00\n" +
				"00 00 02 13 00 00 00 03  0E 00 00 00 3C 00 74 00\n" +
				"6F 00 6B 00 65 00 6E 00  3E 00 FF\n",
		},
		[]string{
			"  04 01 00 20  00 00 01 00   00 00 10 00  06 01 00 16\n" +
				"00 01 06 00  17 00 01 FF   0C 00 07 D0  00 00 02 01\n",
			"  04 01 00 4A  00 00 01 00   AD 32 00 01 74  00 00 04\n" +
				"14 4d 00 69  00 63 00 72   00 6f 00 73  00 6f 00 66\n" +
				"00 74 00 20  00 53 00 51   00 4c 00 20  00 53 00 65\n" +
				"00 72 00 76  00 65 00 72   00 0c 00 07  d0 fd 00 00\n" +
				"00 00 00 00  00 00 00 00   00 00\n",
		},
	)

	conn.Dialer = mock

	_, err = connect(context.Background(), conn, driverInstanceNoProcess.logger, conn.params)
	if err != nil {
		t.Error(err)
	}

	err = <-mock.result
	if err != nil {
		t.Error(err)
	}
}

func TestLoginWithADALUsernamePasswordAuth(t *testing.T) {
	config, err := msdsn.Parse("sqlserver://localhost:1433?Workstation ID=localhost&log=128")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := NewActiveDirectoryTokenConnector(
		config,
		FedAuthADALWorkflowPassword,
		func(ctx context.Context, serverSPN, stsURL string) (string, error) {
			return "<token>", nil
		},
	)
	if err != nil {
		t.Errorf("Unable to parse dummy DSN: %v", err)
	}

	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)

	mock := NewMockTransportDialer(
		[]string{
			"  12 01 00 35 00 00 01 00  00 00 1F 00 06 01 00 25\n" +
				"00 01 02 00 26 00 01 03  00 27 00 04 04 00 2B 00\n" +
				"01 06 00 2C 00 01 ff 00  00 00 00 00 00 00 00 00\n" +
				"00 00 00 00 01\n",
			"  10 01 00 aa 00 00 01 00  a2 00 00 00 04 00 00 74\n" +
				"00 10 00 00 00 00 00 00  00 00 00 00 00 00 00 00\n" +
				"A0 02 00 10 00 00 00 00  00 00 00 00 5e 00 09 00\n" +
				"70 00 00 00 70 00 00 00  70 00 0a 00 84 00 09 00\n" +
				"96 00 04 00 96 00 00 00  96 00 00 00 96 00 00 00\n" +
				"00 00 00 00 00 00 96 00  00 00 96 00 00 00 96 00\n" +
				"00 00 00 00 00 00 6c 00  6f 00 63 00 61 00 6c 00\n" +
				"68 00 6f 00 73 00 74 00  67 00 6f 00 2d 00 6d 00\n" +
				"73 00 73 00 71 00 6c 00  64 00 62 00 6c 00 6f 00\n" +
				"63 00 61 00 6c 00 68 00  6f 00 73 00 74 00 9a 00\n" +
				"00 00 02 02 00 00 00 05  01 ff\n",
			"  08 01 00 1e 00 00 01 00  12 00 00 00 0e 00 00 00\n" +
				"3c 00 74 00 6f 00 6b 00  65 00 6e 00 3e 00\n",
		},
		[]string{
			"  04 01 00 20 00 00 01 00  00 00 10 00 06 01 00 16\n" +
				"00 01 06 00 17 00 01 FF  0C 00 07 D0 00 00 02 01\n",
			"  04 01 00 97 00 00 01 00  EE 8A 00 00 00 02 00 00\n" +
				"00 02 3A 00 00 00 16 00  00 00 01 3A 00 00 00 50\n" +
				"00 00 00 68 00 74 00 74  00 70 00 73 00 3A 00 2F\n" +
				"00 2F 00 64 00 61 00 74  00 61 00 62 00 61 00 73\n" +
				"00 65 00 2E 00 77 00 69  00 6E 00 64 00 6F 00 77\n" +
				"00 73 00 2E 00 6E 00 65  00 74 00 2F 00 68 00 74\n" +
				"00 74 00 70 00 73 00 3A  00 2F 00 2F 00 65 00 78\n" +
				"00 61 00 6D 00 70 00 6C  00 65 00 2E 00 63 00 6F\n" +
				"00 6D 00 2F 00 61 00 75  00 74 00 68 00 6F 00 72\n" +
				"00 69 00 74 00 79 00\n",
			"  04 01 00 4A 00 00 01 00  AD 32 00 01 74 00 00 04\n" +
				"14 4d 00 69 00 63 00 72  00 6f 00 73 00 6f 00 66\n" +
				"00 74 00 20 00 53 00 51  00 4c 00 20 00 53 00 65\n" +
				"00 72 00 76 00 65 00 72  00 0c 00 07 d0 fd 00 00\n" +
				"00 00 00 00 00 00 00 00  00 00\n",
		},
	)

	conn.Dialer = mock

	_, err = connect(context.Background(), conn, driverInstanceNoProcess.logger, conn.params)
	if err != nil {
		t.Error(err)
	}

	err = <-mock.result
	if err != nil {
		t.Error(err)
	}
}

func TestLoginWithADALManagedIdentityAuth(t *testing.T) {
	config, err := msdsn.Parse("sqlserver://localhost:1433?Workstation ID=localhost&log=128")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := NewActiveDirectoryTokenConnector(
		config,
		FedAuthADALWorkflowMSI,
		func(ctx context.Context, serverSPN, stsURL string) (string, error) {
			return "<token>", nil
		},
	)
	if err != nil {
		t.Errorf("Unable to parse dummy DSN: %v", err)
	}

	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)

	mock := NewMockTransportDialer(
		[]string{
			"  12 01 00 35 00 00 01 00  00 00 1F 00 06 01 00 25\n" +
				"00 01 02 00 26 00 01 03  00 27 00 04 04 00 2B 00\n" +
				"01 06 00 2C 00 01 ff 00  00 00 00 00 00 00 00 00\n" +
				"00 00 00 00 01\n",
			"  10 01 00 aa 00 00 01 00  a2 00 00 00 04 00 00 74\n" +
				"00 10 00 00 00 00 00 00  00 00 00 00 00 00 00 00\n" +
				"A0 02 00 10 00 00 00 00  00 00 00 00 5e 00 09 00\n" +
				"70 00 00 00 70 00 00 00  70 00 0a 00 84 00 09 00\n" +
				"96 00 04 00 96 00 00 00  96 00 00 00 96 00 00 00\n" +
				"00 00 00 00 00 00 96 00  00 00 96 00 00 00 96 00\n" +
				"00 00 00 00 00 00 6c 00  6f 00 63 00 61 00 6c 00\n" +
				"68 00 6f 00 73 00 74 00  67 00 6f 00 2d 00 6d 00\n" +
				"73 00 73 00 71 00 6c 00  64 00 62 00 6c 00 6f 00\n" +
				"63 00 61 00 6c 00 68 00  6f 00 73 00 74 00 9a 00\n" +
				"00 00 02 02 00 00 00 05  03 ff\n",
			"  08 01 00 1e 00 00 01 00  12 00 00 00 0e 00 00 00\n" +
				"3c 00 74 00 6f 00 6b 00  65 00 6e 00 3e 00\n",
		},
		[]string{
			"  04 01 00 20 00 00 01 00  00 00 10 00 06 01 00 16\n" +
				"00 01 06 00 17 00 01 FF  0C 00 07 D0 00 00 02 01\n",
			"  04 01 00 97 00 00 01 00  EE 8A 00 00 00 02 00 00\n" +
				"00 02 3A 00 00 00 16 00  00 00 01 3A 00 00 00 50\n" +
				"00 00 00 68 00 74 00 74  00 70 00 73 00 3A 00 2F\n" +
				"00 2F 00 64 00 61 00 74  00 61 00 62 00 61 00 73\n" +
				"00 65 00 2E 00 77 00 69  00 6E 00 64 00 6F 00 77\n" +
				"00 73 00 2E 00 6E 00 65  00 74 00 2F 00 68 00 74\n" +
				"00 74 00 70 00 73 00 3A  00 2F 00 2F 00 65 00 78\n" +
				"00 61 00 6D 00 70 00 6C  00 65 00 2E 00 63 00 6F\n" +
				"00 6D 00 2F 00 61 00 75  00 74 00 68 00 6F 00 72\n" +
				"00 69 00 74 00 79 00\n",
			"  04 01 00 4A 00 00 01 00  AD 32 00 01 74 00 00 04\n" +
				"14 4d 00 69 00 63 00 72  00 6f 00 73 00 6f 00 66\n" +
				"00 74 00 20 00 53 00 51  00 4c 00 20 00 53 00 65\n" +
				"00 72 00 76 00 65 00 72  00 0c 00 07 d0 fd 00 00\n" +
				"00 00 00 00 00 00 00 00  00 00\n",
		},
	)

	conn.Dialer = mock

	_, err = connect(context.Background(), conn, driverInstanceNoProcess.logger, conn.params)
	if err != nil {
		t.Error(err)
	}

	err = <-mock.result
	if err != nil {
		t.Error(err)
	}
}
