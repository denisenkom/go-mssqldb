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
		{"[proc\n1]", true},
		{"schema.name", true},
		{"[schema].[name]", true},
		{"schema.[name]", true},
		{"[schema].name", true},
		{"schema.[proc name]", true},
		{"db.schema.[proc name]", true},
		{"db..[proc name]", true},
		{"#temp_@_proc", true},
		{"_temp.[_proc]", true},
		{"raiserror(13000,1,1)", false},
		{"select*from(@table)", false},
		{"select[A]]]+1from[B]", false},
		{"--proc", false},
		{"[proc;]", true},
		{" proc", false},
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
	_, err = convertIsolationLevel(sql.LevelWriteCommitted)
	if err == nil {
		t.Fatal("must fail but it didn't")
	}
	_, err = convertIsolationLevel(sql.LevelLinearizable)
	if err == nil {
		t.Fatal("must fail but it didn't")
	}
	_, err = convertIsolationLevel(sql.IsolationLevel(1000))
	if err == nil {
		t.Fatal("must fail but it didn't")
	}
}
