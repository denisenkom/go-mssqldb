package mssql

import (
	"encoding/hex"
	"net"
	"strings"
	"time"
)

type connLogger struct {
	conn                  net.Conn
	readKind, writeKind   string
	readCount, writeCount int
	logger                Logger
}

var _ net.Conn = &connLogger{}

func newConnLogger(conn net.Conn, kind string, logger Logger) net.Conn {
	if len(kind) > 0 && !strings.HasPrefix(kind, " ") {
		kind = " " + kind
	}

	cl := &connLogger{
		conn:      conn,
		readKind:  "R" + kind,
		writeKind: "W" + kind,
		logger:    logger,
	}

	return cl
}

func (cl *connLogger) Read(p []byte) (n int, err error) {
	n, err = cl.conn.Read(p)

	if n > 0 {
		dump := hex.Dump(p)
		cl.logger.Printf("%s %d\n%s", cl.readKind, cl.readCount, dump)
		cl.readCount += n
	}

	return
}

func (cl *connLogger) Write(p []byte) (n int, err error) {
	n, err = cl.conn.Write(p)

	if n > 0 {
		dump := hex.Dump(p)
		cl.logger.Printf("%s %d\n%s", cl.writeKind, cl.writeCount, dump)
		cl.writeCount += n
	}

	return
}

func (cl *connLogger) Close() (err error) {
	return cl.conn.Close()
}

func (cl *connLogger) LocalAddr() net.Addr {
	return cl.conn.LocalAddr()
}

func (cl *connLogger) RemoteAddr() net.Addr {
	return cl.conn.RemoteAddr()
}

func (cl *connLogger) SetDeadline(t time.Time) error {
	return cl.conn.SetDeadline(t)
}

func (cl *connLogger) SetReadDeadline(t time.Time) error {
	return cl.conn.SetReadDeadline(t)
}

func (cl *connLogger) SetWriteDeadline(t time.Time) error {
	return cl.conn.SetWriteDeadline(t)
}
