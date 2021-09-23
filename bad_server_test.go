package mssql

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
)

// tests simulating bad server

func testConnectionBad(t *testing.T, connStr string) {
	conn, err := sql.Open("mssql", connStr)
	if err != nil {
		// should not fail here
		t.Fatal("Open connection failed:", err.Error())
		return
	}
	defer conn.Close()
	row := conn.QueryRow("select 1")
	var val int
	err = row.Scan(&val)
	if err == nil {
		t.Fatal("Scan should fail but it succeeded")
	}
}

func testBadServer(t *testing.T, handler func(net.Conn)) {
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
	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)
	testConnectionBad(t, fmt.Sprintf("host=%s;port=%d;log=255", addr.IP.String(), addr.Port))
}

func TestBadServerCloseConnection(t *testing.T) {
	testBadServer(t, func(conn net.Conn) {})
}

func TestBadServerInvalidPreLoginPacketSize(t *testing.T) {
	testBadServer(t, func(conn net.Conn) {
		// indicate to the client that this is not a final packet
		// but since there are no more data written, client would fail
		err := binary.Write(conn, binary.BigEndian, header{
			PacketType: packReply,
			Size:       uint16(headerSize),
			Status:     0, // indicates non final packet
		})
		if err != nil {
			t.Fatal("Writing header failed", err)
		}
	})
}

func TestBadServerInvalidPreLoginPacketType(t *testing.T) {
	testBadServer(t, func(conn net.Conn) {
		err := binary.Write(conn, binary.BigEndian, header{
			PacketType: packNormal, // invalid packet type, packReply
			Size:       uint16(headerSize),
			Status:     1, // indicate final packet
		})
		if err != nil {
			t.Fatal("Writing header failed", err)
		}
	})
}

func TestBadServerEmptyPreLoginPacket(t *testing.T) {
	testBadServer(t, func(conn net.Conn) {
		err := binary.Write(conn, binary.BigEndian, header{
			PacketType: packReply,
			Size:       uint16(headerSize),
			Status:     1, // indicate final packet
		})
		if err != nil {
			t.Fatal("Writing header failed", err)
		}
	})
}

func TestBadServerPreLoginPacketWithNoEntries(t *testing.T) {
	testBadServer(t, func(conn net.Conn) {
		buf := newTdsBuffer(defaultPacketSize, conn)
		fields := map[uint8][]byte{}
		err := writePrelogin(packReply, buf, fields)
		if err != nil {
			t.Fatal("Writing PRELOGIN packet failed", err)
		}
	})
}

func TestBadServerPreLoginPacketWithJustEncryptionField(t *testing.T) {
	testBadServer(t, func(conn net.Conn) {
		buf := newTdsBuffer(defaultPacketSize, conn)
		fields := map[uint8][]byte{
			preloginENCRYPTION: {encryptNotSup},
		}
		err := writePrelogin(packReply, buf, fields)
		if err != nil {
			t.Fatal("Writing PRELOGIN packet failed", err)
		}
	})
}

func goodPreloginSequence(t *testing.T, buf *tdsBuffer) {
	// read prelogin request
	packetType, err := buf.BeginRead()
	if err != nil {
		t.Fatal("Failed to read PRELOGIN request", err)
	}
	if packetType != packPrelogin {
		t.Fatal("Client sent non PRELOGIN request packet type", packetType)
	}

	// write prelogin response
	fields := map[uint8][]byte{
		preloginENCRYPTION: {encryptNotSup},
	}
	err = writePrelogin(packReply, buf, fields)
	if err != nil {
		t.Fatal("Writing PRELOGIN packet failed", err)
	}

	// read login request
	packetType, err = buf.BeginRead()
	if err != nil {
		t.Fatal("Failed to read LOGIN request", err)
	}
	if packetType != packLogin7 {
		t.Fatal("Client sent non LOGIN request packet type", packetType)
	}
}

func TestBadServerNoLoginResponse(t *testing.T) {
	testBadServer(t, func(conn net.Conn) {
		buf := newTdsBuffer(defaultPacketSize, conn)

		goodPreloginSequence(t, buf)

		// not sending login response
	})
}

func TestBadServerIncorrectLoginResponseType(t *testing.T) {
	testBadServer(t, func(conn net.Conn) {
		buf := newTdsBuffer(defaultPacketSize, conn)

		goodPreloginSequence(t, buf)

		// sending incorrect packet type
		buf.BeginPacket(packPrelogin, false)
		err := buf.flush()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestBadServerInvalidTokenId(t *testing.T) {
	testBadServer(t, func(conn net.Conn) {
		buf := newTdsBuffer(defaultPacketSize, conn)

		goodPreloginSequence(t, buf)

		// sending reply to LOGIN request
		buf.BeginPacket(packReply, false)
		// this is invalid token id
		err := buf.WriteByte(0)
		if err != nil {
			t.Fatal(err)
		}
		err = buf.flush()
		if err != nil {
			t.Fatal(err)
		}
	})
}
