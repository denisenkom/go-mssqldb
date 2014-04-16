package mssql

import (
	"net"
	"time"
)

type timeoutConn struct {
	c       net.Conn
	timeout time.Duration
}

func (c timeoutConn) Read(b []byte) (n int, err error) {
	err = c.c.SetDeadline(time.Now().Add(c.timeout))
	if err != nil {
		return
	}
	return c.c.Read(b)
}

func (c timeoutConn) Write(b []byte) (n int, err error) {
	err = c.c.SetDeadline(time.Now().Add(c.timeout))
	if err != nil {
		return
	}
	return c.c.Write(b)
}

func (c timeoutConn) Close() error {
	return c.c.Close()
}
