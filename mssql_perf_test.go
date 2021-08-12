package mssql

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"testing"
)

// tests performance of the most common operations

func runTestServer(t *testing.B, handler func(net.Conn)) *Conn {
	addr := &net.TCPAddr{IP: net.IP{127, 0, 0, 1}}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatal("Cannot start a listener", err)
	}
	addr = listener.Addr().(*net.TCPAddr)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			t.Log("Failed to accept connection", err)
			return
		}
		handler(conn)
		_ = conn.Close()
	}()
	connStr := fmt.Sprintf("host=%s;port=%d", addr.IP.String(), addr.Port)
	conn, err := driverInstance.open(context.Background(), connStr)
	if err != nil {
		// should not fail here
		t.Fatal("Open connection failed:", err.Error())
	}
	return conn
}

func testConnClose(t *testing.B, conn *Conn) {
	err := conn.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkSelect(b *testing.B) {
	// Benchmark select query against mock server
	conn := runTestServer(b, func(conn net.Conn) {
		tdsBuf := newTdsBuffer(defaultPacketSize, conn)

		// read prelogin request
		packetType, err := tdsBuf.BeginRead()
		if err != nil {
			b.Fatal("Failed to read PRELOGIN request", err)
		}
		if packetType != packPrelogin {
			b.Fatal("Client sent non PRELOGIN request packet type", packetType)
		}

		// write prelogin response
		fields := map[uint8][]byte{
			preloginENCRYPTION: {encryptNotSup},
		}
		err = writePrelogin(packReply, tdsBuf, fields)
		if err != nil {
			b.Fatal("Writing PRELOGIN packet failed", err)
		}

		// read login request
		packetType, err = tdsBuf.BeginRead()
		if err != nil {
			b.Fatal("Failed to read LOGIN request", err)
		}
		if packetType != packLogin7 {
			b.Fatal("Client sent non LOGIN request packet type", packetType)
		}
		_, err = ioutil.ReadAll(tdsBuf)
		if err != nil {
			b.Fatal(err)
		}

		// send login response
		tdsBuf.BeginPacket(packReply, false)
		buf := make([]byte, 1+2+2+8)
		buf[0] = byte(tokenDone)
		binary.LittleEndian.PutUint16(buf[1:], 0)
		binary.LittleEndian.PutUint16(buf[3:], 0)
		binary.LittleEndian.PutUint64(buf[5:], 0)
		_, err = tdsBuf.Write(buf)
		if err != nil {
			b.Log("writing login reply failed", err)
			return
		}
		err = tdsBuf.FinishPacket()
		if err != nil {
			b.Log("writing login reply failed", err)
			return
		}
		// this is response for select 1 request
		selectResponseBytes, err := base64.StdEncoding.DecodeString("gQEAAAAAACAAOADRAQAAAP0QAMEAAQAAAAAAAAA=")
		if err != nil {
			b.Fatal(err)
		}

		for requests := 0; ; requests++ {
			// read request
			_, err = tdsBuf.BeginRead()
			if err != nil {
				b.Log(err)
				return
			}
			_, err = ioutil.ReadAll(tdsBuf)
			if err != nil {
				b.Log(err)
				return
			}

			// send response
			tdsBuf.BeginPacket(packReply, false)
			if err != nil {
				b.Log(err)
				return
			}
			_, err = tdsBuf.Write(selectResponseBytes)
			if err != nil {
				b.Log("writing login reply failed", err)
				return
			}
			err = tdsBuf.FinishPacket()
			if err != nil {
				b.Log("writing login reply failed", err)
				return
			}
		}
	})
	defer testConnClose(b, conn)

	values := make([]driver.Value, 1)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		stmt, err := conn.prepareContext(ctx, "select 1")
		if err != nil {
			b.Fatal(err)
		}
		rows, err := stmt.queryContext(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}

		err = rows.Next(values)
		if err != nil {
			b.Fatal(err)
		}

		err = rows.Next(values)
		if err != io.EOF {
			b.Fatal("there should not be a second row")
		}

		err = rows.Close()
		if err != nil {
			b.Fatal(err)
		}
		err = stmt.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

type onlyReadTransport struct {
	b   *testing.B
	rdr *bytes.Reader
}

func (transport onlyReadTransport) Read(p []byte) (n int, err error) {
	return transport.rdr.Read(p)
}

func (transport onlyReadTransport) Write(p []byte) (int, error) {
	return 0, errors.New("unexpected write")
}

func (transport onlyReadTransport) Close() error {
	return errors.New("unexpected close")
}

func BenchmarkSelectParser(b *testing.B) {
	sess := &tdsSession{
		buf: newTdsBuffer(defaultPacketSize, nil),
	}
	// this is response for select 1 request
	selectResponseBytes, err := base64.StdEncoding.DecodeString("gQEAAAAAACAAOADRAQAAAP0QAMEAAQAAAAAAAAA=")
	if err != nil {
		b.Fatal(err)
	}
	rdr := bytes.NewReader(selectResponseBytes)
	sess.buf.transport = onlyReadTransport{
		rdr: rdr,
		b:   b,
	}
	for i := 0; i < b.N; i++ {
		ch := make(chan tokenStruct, 5)
		_, err = rdr.Seek(0, io.SeekStart)
		if err != nil {
			b.Fatal(err)
		}
		processSingleResponse(context.Background(), sess, ch, outputs{})
	}
}
