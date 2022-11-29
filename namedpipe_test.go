//go:build np
// +build np

package mssql

import (
	"github.com/microsoft/go-mssqldb/msdsn"
	_ "github.com/microsoft/go-mssqldb/namedpipe"
	"testing"
)

func TestNamedPipeProtocolInstalled(t *testing.T) {
	if len(msdsn.ProtocolParsers) != 2 {
		t.Fatal("np protocol not registered")
	}
}
