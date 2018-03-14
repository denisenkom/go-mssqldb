package mssql

import (
	"database/sql"
	"net/url"
	"testing"
)

func TestBadConnect(t *testing.T) {
	checkConnStr(t)
	SetLogger(testLogger{t})
	connURL := makeConnStr(t)
	connURL.User = url.UserPassword("baduser", "badpwd")
	badDSN := connURL.String()

	conn, err := sql.Open("mssql", badDSN)
	if err != nil {
		t.Error("Open connection failed:", err.Error())
	}
	defer conn.Close()

	err = conn.Ping()
	if err == nil {
		t.Error("Ping should fail for connection: ", badDSN)
	}
}
