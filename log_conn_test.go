package mssql

import (
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnLoggerOperations(t *testing.T) {
	clt := &connLoggerTest{}
	cl := newConnLogger(clt, "test", nullLogger{})
	packet := append(make([]byte, 0, 10), 1, 2, 3, 4, 5)
	n, err := cl.Read(packet)
	if n != 10 || err != nil {
		t.Error("Unexpected return value from call to Read()")
	}

	n, err = cl.Write(packet)
	if n != 5 || err != nil {
		t.Error("Unexpected return value from call to Write()")
	}

	if cl.Close() != nil {
		t.Error("Unexpected return value from call to Close()")
	}

	if cl.LocalAddr() == nil {
		t.Error("Unexpected return value from call to LocalAddr()")
	}

	if cl.RemoteAddr() == nil {
		t.Error("Unexpected return value from call to RemoteAddr()")
	}

	if cl.SetDeadline(time.Now()) != nil {
		t.Error("Unexpected return value from call to SetDeadline()")
	}

	if cl.SetReadDeadline(time.Now()) != nil {
		t.Error("Unexpected return value from call to SetReadDeadline()")
	}

	if cl.SetWriteDeadline(time.Now()) != nil {
		t.Error("Unexpected return value from call to SetWriteDeadline()")
	}

	if atomic.LoadInt32(&clt.calls) != 8 {
		t.Error("Unexpected number of calls recorded")
	}
}

type connLoggerTest struct {
	calls int32
}

var _ net.Conn = &connLoggerTest{}

type addressTest struct {
}

var _ net.Addr = &addressTest{}

type nullLogger struct {
}

var _ Logger = nullLogger{}

func (n nullLogger) Printf(format string, v ...interface{}) {
}

func (n nullLogger) Println(v ...interface{}) {
}

func (a *addressTest) Network() string {
	return "test"
}

func (a *addressTest) String() string {
	return "test"
}

func (cl *connLoggerTest) Read(p []byte) (int, error) {
	atomic.AddInt32(&cl.calls, 1)
	return cap(p), nil
}

func (cl *connLoggerTest) Write(p []byte) (int, error) {
	atomic.AddInt32(&cl.calls, 1)
	return len(p), nil
}

func (cl *connLoggerTest) Close() error {
	atomic.AddInt32(&cl.calls, 1)
	return nil
}

func (cl *connLoggerTest) LocalAddr() net.Addr {
	atomic.AddInt32(&cl.calls, 1)
	return &addressTest{}
}

func (cl *connLoggerTest) RemoteAddr() net.Addr {
	atomic.AddInt32(&cl.calls, 1)
	return &addressTest{}
}

func (cl *connLoggerTest) SetDeadline(t time.Time) error {
	atomic.AddInt32(&cl.calls, 1)
	return nil
}

func (cl *connLoggerTest) SetReadDeadline(t time.Time) error {
	atomic.AddInt32(&cl.calls, 1)
	return nil
}

func (cl *connLoggerTest) SetWriteDeadline(t time.Time) error {
	atomic.AddInt32(&cl.calls, 1)
	return nil
}
