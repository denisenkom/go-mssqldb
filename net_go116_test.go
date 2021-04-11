// +build go1.16

package mssql

import (
	"context"
	"net"
	"testing"
	"time"
)

func assertPanic(t *testing.T, paniced bool) {
	v := recover()
	if paniced && v == nil {
		t.Fatalf(`expected panic but it did not`)
	}

	if !paniced && v != nil {
		t.Fatalf(`expected no panic but it did`)
	}
}

func TestTimeoutConn(t *testing.T) {
	_, conn := net.Pipe()

	tconn := newTimeoutConn(conn, time.Minute)
	t.Run(`set deadline`, func(t *testing.T) {
		defer assertPanic(t, false)
		deadline := time.Now().Add(time.Millisecond * 100)

		err := tconn.SetDeadline(deadline)
		if err != nil {
			t.Fatalf(`SetDeadline should return nil`)
		}
	})

	t.Run(`set read deadline`, func(t *testing.T) {
		defer assertPanic(t, false)
		deadline := time.Now().Add(time.Minute)

		err := tconn.SetReadDeadline(deadline)
		if err != nil {
			t.Fatalf(`SetReadDeadline should return nil`)
		}
	})

	t.Run(`set write deadline`, func(t *testing.T) {
		defer assertPanic(t, false)
		deadline := time.Now().Add(time.Minute)

		err := tconn.SetWriteDeadline(deadline)
		if err != nil {
			t.Fatalf(`SetWriteDeadline should return nil`)
		}
	})
}

func TestTLSHandshakeConn(t *testing.T) {
	SetLogger(testLogger{t})

	connector, err := NewConnector(makeConnStr(t).String())
	if err != nil {
		t.Error(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	toconn, err := dialConnection(ctx, connector, connector.params)
	if err != nil {
		t.Error(err)
	}

	outbuf := newTdsBuffer(connector.params.packetSize, toconn)
	handshakeConn := tlsHandshakeConn{buf: outbuf}

	t.Run(`set deadline`, func(t *testing.T) {
		defer assertPanic(t, false)
		deadline := time.Now().Add(time.Millisecond * 100)

		err := handshakeConn.SetDeadline(deadline)
		if err != nil {
			t.Fatalf(`SetDeadline should return nil`)
		}
	})

	t.Run(`set read deadline`, func(t *testing.T) {
		defer assertPanic(t, false)
		deadline := time.Now().Add(time.Minute)

		err := handshakeConn.SetReadDeadline(deadline)
		if err != nil {
			t.Fatalf(`SetReadDeadline should return nil`)
		}
	})

	t.Run(`set write deadline`, func(t *testing.T) {
		defer assertPanic(t, false)
		deadline := time.Now().Add(time.Minute)

		err := handshakeConn.SetWriteDeadline(deadline)
		if err != nil {
			t.Fatalf(`SetWriteDeadline should return nil`)
		}
	})

	t.Run(`get remote addr`, func(t *testing.T) {
		addr := handshakeConn.RemoteAddr()
		if addr != nil {
			t.Fatalf(`RemoteAddr should return nil`)
		}
	})
}

func TestPassthroughConn(t *testing.T) {
	SetLogger(testLogger{t})

	connector, err := NewConnector(makeConnStr(t).String())
	if err != nil {
		t.Error(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	toconn, err := dialConnection(ctx, connector, connector.params)
	if err != nil {
		t.Error(err)
	}

	outbuf := newTdsBuffer(connector.params.packetSize, toconn)

	handshakeConn := tlsHandshakeConn{buf: outbuf}
	passthrough := passthroughConn{c: &handshakeConn}

	t.Run(`set deadline`, func(t *testing.T) {
		defer assertPanic(t, false)
		deadline := time.Now().Add(time.Millisecond * 100)

		err := passthrough.SetDeadline(deadline)
		if err != nil {
			t.Fatalf(`SetDeadline should return nil`)
		}
	})

	t.Run(`set read deadline`, func(t *testing.T) {
		defer assertPanic(t, false)
		deadline := time.Now().Add(time.Minute)

		err := passthrough.SetReadDeadline(deadline)
		if err != nil {
			t.Fatalf(`SetReadDeadline should return nil`)
		}
	})

	t.Run(`set write deadline`, func(t *testing.T) {
		defer assertPanic(t, false)
		deadline := time.Now().Add(time.Minute)

		err := passthrough.SetWriteDeadline(deadline)
		if err != nil {
			t.Fatalf(`SetWriteDeadline should return nil`)
		}
	})

	t.Run(`get remote addr`, func(t *testing.T) {
		addr := passthrough.RemoteAddr()
		if addr != nil {
			t.Fatalf(`RemoteAddr should return nil`)
		}
	})
}
