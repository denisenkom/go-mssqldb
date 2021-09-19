// +build go1.10

package mssql

import (
	"database/sql"
	"testing"
)

func open(t *testing.T) (*sql.DB, *testLogger) {
	tl := testLogger{t: t}
	SetLogger(&tl)
	checkConnStr(t)
	connector, err := NewConnector(makeConnStr(t).String())
	if err != nil {
		t.Error("Open connection failed:", err.Error())
		return nil, &tl
	}
	conn := sql.OpenDB(connector)
	return conn, &tl
}
