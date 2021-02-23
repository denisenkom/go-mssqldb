package mssql

import (
	"context"
	"database/sql"
	"testing"
)

func TestBadOpen(t *testing.T) {
	drv := driverWithProcess(t)
	_, err := drv.open(context.Background(), "port=bad")
	if err == nil {
		t.Fail()
	}
}

func TestIsProc(t *testing.T) {
	list := []struct {
		s  string
		is bool
	}{
		{"proc", true},
		{"select 1;", false},
		{"select 1", false},
		{"[proc 1]", true},
		{"[proc\n1]", false},
		{"schema.name", true},
		{"[schema].[name]", true},
		{"schema.[name]", true},
		{"[schema].name", true},
		{"schema.[proc name]", true},
	}

	for _, item := range list {
		got := isProc(item.s)
		if got != item.is {
			t.Errorf("for %q, got %t want %t", item.s, got, item.is)
		}
	}
}

func TestConvertIsolationLevel(t *testing.T) {
	level, err := convertIsolationLevel(sql.LevelReadUncommitted)
	if level != isolationReadUncommited || err != nil {
		t.Fatal("invalid value returned")
	}
	level, err = convertIsolationLevel(sql.LevelReadCommitted)
	if level != isolationReadCommited || err != nil {
		t.Fatal("invalid value returned")
	}
	level, err = convertIsolationLevel(sql.LevelRepeatableRead)
	if level != isolationRepeatableRead || err != nil {
		t.Fatal("invalid value returned")
	}
	level, err = convertIsolationLevel(sql.LevelSnapshot)
	if level != isolationSnapshot || err != nil {
		t.Fatal("invalid value returned")
	}
	level, err = convertIsolationLevel(sql.LevelWriteCommitted)
	if err == nil {
		t.Fatal("must fail but it didn't")
	}
	level, err = convertIsolationLevel(sql.LevelLinearizable)
	if err == nil {
		t.Fatal("must fail but it didn't")
	}
	level, err = convertIsolationLevel(sql.IsolationLevel(1000))
	if err == nil {
		t.Fatal("must fail but it didn't")
	}
}