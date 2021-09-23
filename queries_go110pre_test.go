// +build !go1.10

package mssql

import (
	"testing"
)

func TestIdentity(t *testing.T) {
	conn, logger := open(t)
	defer conn.Close()
	defer logger.StopLogging()

	tx, err := conn.Begin()
	if err != nil {
		t.Fatal("Begin tran failed", err)
	}
	defer tx.Rollback()

	res, err := tx.Exec("create table #foo (bar int identity, baz int unique)")
	if err != nil {
		t.Fatal("create table failed")
	}

	res, err = tx.Exec("insert into #foo (baz) values (1)")
	if err != nil {
		t.Fatal("insert failed")
	}
	n, err := res.LastInsertId()
	if err != nil {
		t.Fatal("last insert id failed")
	}
	if n != 1 {
		t.Error("Expected 1 for identity, got ", n)
	}

	res, err = tx.Exec("insert into #foo (baz) values (20)")
	if err != nil {
		t.Fatal("insert failed")
	}
	n, err = res.LastInsertId()
	if err != nil {
		t.Fatal("last insert id failed")
	}
	if n != 2 {
		t.Error("Expected 2 for identity, got ", n)
	}

	res, err = tx.Exec("insert into #foo (baz) values (1)")
	if err == nil {
		t.Fatal("insert should fail")
	}

	res, err = tx.Exec("insert into #foo (baz) values (?)", 1)
	if err == nil {
		t.Fatal("insert should fail")
	}
}
