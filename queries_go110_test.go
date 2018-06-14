// +build go1.10

package mssql

import (
	"context"
	"database/sql"
	"testing"
	"time"
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

func TestParameterTypes(t *testing.T) {
	pool, err := sql.Open("sqlserver", makeConnStr(t).String())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	tin, err := time.Parse(time.RFC3339, "2006-01-02T22:04:05-07:00")
	if err != nil {
		t.Fatal(err)
	}

	var nv, v, dt1, dto string
	err = pool.QueryRow(`
select
	nv = SQL_VARIANT_PROPERTY(@nv,'BaseType'),
	v = SQL_VARIANT_PROPERTY(@v,'BaseType'),
	dt1 = SQL_VARIANT_PROPERTY(@dt1,'BaseType'),
	dto = SQL_VARIANT_PROPERTY(@dto,'BaseType')
;
	`,
		sql.Named("nv", "base type nvarchar"),
		sql.Named("v", VarChar("base type varchar")),
		sql.Named("dt1", DateTime1(tin)),
		sql.Named("dto", DateTimeOffset(tin)),
	).Scan(&nv, &v, &dt1, &dto)
	if err != nil {
		t.Fatal(err)
	}
	if nv != "nvarchar" {
		t.Errorf(`want "nvarchar" got %q`, nv)
	}
	if v != "varchar" {
		t.Errorf(`want "varchar" got %q`, v)
	}
	if dt1 != "datetime" {
		t.Errorf(`want "datetime" got %q`, dt1)
	}
	if dto != "datetimeoffset" {
		t.Errorf(`want "datetimeoffset" got %q`, dto)
	}
}

func TestParameterValues(t *testing.T) {
	pool, err := sql.Open("sqlserver", makeConnStr(t).String())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	sin := "high five"
	tin, err := time.Parse(time.RFC3339, "2006-01-02T22:04:05-07:00")
	if err != nil {
		t.Fatal(err)
	}

	var nv, v, tgo, dt1, dto string
	err = pool.QueryRow(`
select
	nv = @nv,
	v = @v,
	tgo = @tgo,
	dt1 = convert(nvarchar(200), @dt1, 121),
	dto = convert(nvarchar(200), @dto, 121)
;
	`,
		sql.Named("nv", sin),
		sql.Named("v", sin),
		sql.Named("tgo", tin),
		sql.Named("dt1", DateTime1(tin)),
		sql.Named("dto", DateTimeOffset(tin)),
	).Scan(&nv, &v, &tgo, &dt1, &dto)
	if err != nil {
		t.Fatal(err)
	}
	if want := sin; nv != want {
		t.Errorf(`want %q got %q`, want, nv)
	}
	if want := sin; v != want {
		t.Errorf(`want %q got %q`, want, v)
	}
	if want := "2006-01-02T22:04:05-07:00"; tgo != want {
		t.Errorf(`want %q got %q`, want, tgo)
	}
	if want := "2006-01-02 22:04:05.000"; dt1 != want {
		t.Errorf(`want %q got %q`, want, dt1)
	}
	if want := "2006-01-02 22:04:05.0000000 -07:00"; dto != want {
		t.Errorf(`want %q got %q`, want, dto)
	}
}
