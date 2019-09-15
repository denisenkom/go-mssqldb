// +build !go1.10

package mssql

import (
	"database/sql"
	"testing"
)

func open(t *testing.T) *sql.DB {
	checkConnStr(t)
	SetLogger(testLogger{t})
	conn, err := sql.Open("sqlserver", makeConnStr(t).String())
	if err != nil {
	t.Error("Open connection failed:", err.Error())
		return nil
	}
	return conn
}