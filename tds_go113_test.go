//go:build go1.13
// +build go1.13

package mssql

import (
	"database/sql"
	"errors"
	"net"
	"testing"

	"github.com/microsoft/go-mssqldb/msdsn"
)

// TestConnectError tests wrapped errors from connection establishing. It uses
// error handling introduced in Go 1.13, that's the reason for conditional test.
func TestConnectError(t *testing.T) {
	loadConnParams := func(t *testing.T) msdsn.Config {
		params := testConnParams(t)
		if params.Encryption == msdsn.EncryptionRequired {
			t.Skip("Unable to test connection to IP for servers that expect encryption")
		}
		p, ok := params.Parameters["protocol"]
		if ok && p != "tcp" {
			t.Skip("Only works for tcp errors")
		}
		// clear instance name, so we don't tease SQL Server Browser.
		params.Instance = ""

		if params.Host == "." {
			params.Host = "127.0.0.1"
		} else {
			ips, err := net.LookupIP(params.Host)
			if err != nil {
				t.Fatal("Unable to lookup IP", err)
			}
			params.Host = ips[0].String()
		}
		return params
	}
	connAndPing := func(t *testing.T, params msdsn.Config) error {
		connStr := params.URL().String()
		conn, err := sql.Open("mssql", connStr)
		if err != nil {
			t.Fatal("Open connection failed:", err.Error())
			return nil
		}
		pingErr := conn.Ping()
		if pingErr == nil {
			t.Fatal("Error required")
			return nil
		}
		return pingErr
	}
	t.Run("bad port - refused connection", func(t *testing.T) {
		params := loadConnParams(t)
		// port where nothing listens on. Port 666 is reserved for Doom multiplayer
		// server, hopefully no-one runs one in CI or in development environment.
		params.Port = 666

		connErr := connAndPing(t, params)

		var ne *net.OpError
		if !errors.As(connErr, &ne) {
			t.Fatalf("Expected *net.OpError, got: %[1]T: %[1]v", connErr)
			return
		}
		if ne.Op != "dial" {
			t.Fatalf("Expected net dial error: %v", connErr)
			return
		}
		if ne.Timeout() {
			t.Fatalf("Expected not timeout error: %v", connErr)
			return
		}
	})
	t.Run("bad addr - host will keep us hanging", func(t *testing.T) {
		params := loadConnParams(t)
		// Change host to server that won't talk to us and will keep the connection
		// hanging.
		params.Host = "8.8.8.8"

		connErr := connAndPing(t, params)

		var ne *net.OpError
		if !errors.As(connErr, &ne) {
			t.Fatalf("Expected *net.OpError, got: %[1]T: %[1]v", connErr)
			return
		}
		if ne.Op != "dial" {
			t.Fatalf("Expected net dial error: %v", connErr)
			return
		}
		if !ne.Timeout() {
			t.Fatalf("Expected timeout error: %v", connErr)
			return
		}
	})
}
