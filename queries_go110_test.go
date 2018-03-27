// +build go1.10

package mssql

import (
	"context"
	"database/sql"
	"testing"
)

func TestSessionInitSQL(t *testing.T) {
	checkConnStr(t)
	SetLogger(testLogger{t})

	d := &Driver{}
	connector, err := d.OpenConnector(makeConnStr(t).String())
	if err != nil {
		t.Fatal("unable to open connector", err)
	}

	// Do not use these settings in your application
	// unless you know what they do.
	// Thes are for this unit test only.
	//
	// Sessions will be reset even if SessionInitSQL is not set.
	connector.SessionInitSQL = `
SET XACT_ABORT ON; -- 16384
SET ANSI_NULLS ON; -- 32
SET ARITHIGNORE ON; -- 128
`

	pool := sql.OpenDB(connector)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var opt int32
	err = pool.QueryRowContext(ctx, `
select Options = @@OPTIONS;
`).Scan(&opt)
	if err != nil {
		t.Fatal("failed to run query", err)
	}
	mask := int32(16384 | 128 | 32)

	if opt&mask != mask {
		t.Fatal("incorrect session settings", opt)
	}
}
