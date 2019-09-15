// +build go1.10

package mssql

import (
	"database/sql"
	"testing"
)

func open(t *testing.T) *sql.DB {
	checkConnStr(t)
	SetLogger(testLogger{t})
	connector, err := NewConnector(makeConnStr(t).String())
	if err != nil {
		t.Error("Open connection failed:", err.Error())
	return nil
	}
	return sql.OpenDB(connector)
}
