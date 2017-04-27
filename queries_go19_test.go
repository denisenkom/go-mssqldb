// +build go1.9

package mssql

import (
	"context"
	"database/sql"
	"testing"
)

func TestOutputParam(t *testing.T) {
	sqltextcreate := `
CREATE PROCEDURE abassign
   @aid INT,
   @bid INT OUTPUT
AS
BEGIN
   SELECT @bid = @aid
END;
`
	sqltextdrop := `DROP PROCEDURE abassign;`
	sqltextrun := `abassign`

	checkConnStr(t)
	SetLogger(testLogger{t})

	db, err := sql.Open("sqlserver", makeConnStr(t).String())
	if err != nil {
		t.Fatalf("failed to open driver sqlserver")
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db.ExecContext(ctx, sqltextdrop)
	_, err = db.ExecContext(ctx, sqltextcreate)
	if err != nil {
		t.Fatal(err)
	}
	var bout int64
	_, err = db.ExecContext(ctx, sqltextrun,
		sql.Named("aid", 5),
		sql.Named("bid", sql.Out{Dest: &bout}),
	)
	defer db.ExecContext(ctx, sqltextdrop)
	if err != nil {
		t.Error(err)
	}

	if bout != 5 {
		t.Errorf("expected 5, got %d", bout)
	}
}

func TestOutputINOUTParam(t *testing.T) {
	sqltextcreate := `
CREATE PROCEDURE abinout
   @aid INT,
   @bid INT OUTPUT
AS
BEGIN
   SELECT @bid = @aid + @bid;
END;
`
	sqltextdrop := `DROP PROCEDURE abinout;`
	sqltextrun := `abinout`

	checkConnStr(t)
	SetLogger(testLogger{t})

	db, err := sql.Open("sqlserver", makeConnStr(t).String())
	if err != nil {
		t.Fatalf("failed to open driver sqlserver")
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db.ExecContext(ctx, sqltextdrop)
	_, err = db.ExecContext(ctx, sqltextcreate)
	if err != nil {
		t.Fatal(err)
	}
	var bout int64 = 3
	_, err = db.ExecContext(ctx, sqltextrun,
		sql.Named("aid", 5),
		sql.Named("bid", sql.Out{Dest: &bout}),
	)
	defer db.ExecContext(ctx, sqltextdrop)
	if err != nil {
		t.Error(err)
	}

	if bout != 8 {
		t.Errorf("expected 8, got %d", bout)
	}
}
