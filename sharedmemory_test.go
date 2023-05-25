//go:build sm
// +build sm

package mssql

import (
	"testing"

	"github.com/microsoft/go-mssqldb/msdsn"
	_ "github.com/microsoft/go-mssqldb/sharedmemory"
)

func TestSharedMemoryProtocolInstalled(t *testing.T) {
	for _, p := range msdsn.ProtocolParsers {
		if p.Protocol() == "lpc" {
			return
		}
	}
	t.Fatalf("ProtocolParsers is missing lpc %v", msdsn.ProtocolParsers)
}

func TestSharedMemoryConnection(t *testing.T) {
	params := testConnParams(t)
	protocol, ok := params.Parameters["protocol"]
	if !ok || protocol != "lpc" {
		t.Skip("Test is not running with named pipe protocol set")
	}
	conn, _ := open(t)
	defer conn.Close()
	row := conn.QueryRow("SELECT net_transport FROM sys.dm_exec_connections WHERE session_id = @@SPID")
	if err := row.Scan(&protocol); err != nil {
		t.Fatalf("Unable to query connection protocol %s", err.Error())
	}
	if protocol != "Shared memory" {
		t.Fatalf("Shared memory connection not made. Protocol: %s", protocol)
	}
}
