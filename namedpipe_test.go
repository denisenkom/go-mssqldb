//go:build np
// +build np

package mssql

import (
	"strings"
	"testing"

	"github.com/microsoft/go-mssqldb/msdsn"
	_ "github.com/microsoft/go-mssqldb/namedpipe"
)

func TestNamedPipeProtocolInstalled(t *testing.T) {
	for _, p := range msdsn.ProtocolParsers {
		if p.Protocol() == "np" {
			return
		}
	}
	t.Fatalf("ProtocolParsers is missing np %v", msdsn.ProtocolParsers)
}

func TestNamedPipeConnection(t *testing.T) {
	params := testConnParams(t)
	protocol, ok := params.Parameters["protocol"]
	if (ok && protocol != "np") || strings.Contains(params.Host, "database.windows.net") {
		t.Skip("Test is not running with named pipe protocol set")
	}
	conn, _ := open(t)
	row := conn.QueryRow(`SELECT net_transport FROM sys.dm_exec_connections WHERE session_id = @@SPID`)
	if err := row.Scan(&protocol); err != nil {
		t.Fatalf("Unable to query connection protocol %s", err.Error())
	}
	if protocol != "Named pipe" {
		t.Fatalf("Named pipe connection not made. Protocol: %s", protocol)
	}
}
