// +build go1.10

package typecivil

import (
	"database/sql"
	"testing"
	"time"

	"github.com/denisenkom/go-mssqldb"
	"github.com/denisenkom/go-mssqldb/internal/testutil"

	"cloud.google.com/go/civil"
)

func newConnector(t *testing.T) *mssql.Connector {
	connector, err := mssql.NewConnector(testutil.MakeConnStr(t).String())
	if err != nil {
		t.Fatal(err)
	}
	return connector
}

func TestParameterTypes(t *testing.T) {
	connector := newConnector(t)
	connector.RegisterExtendedType(Civil)

	pool := sql.OpenDB(connector)
	defer pool.Close()

	tin, err := time.Parse(time.RFC3339, "2006-01-02T22:04:05-07:00")
	if err != nil {
		t.Fatal(err)
	}

	var dt2, tm, d string
	err = pool.QueryRow(`
select
	dt2 = SQL_VARIANT_PROPERTY(@dt2,'BaseType'),
	d = SQL_VARIANT_PROPERTY(@d,'BaseType'),
	tm = SQL_VARIANT_PROPERTY(@tm,'BaseType')
;
	`,
		sql.Named("dt2", civil.DateTimeOf(tin)),
		sql.Named("d", civil.DateOf(tin)),
		sql.Named("tm", civil.TimeOf(tin)),
	).Scan(&dt2, &d, &tm)
	if err != nil {
		t.Fatal(err)
	}
	if dt2 != "datetime2" {
		t.Errorf(`want "datetime2" got %q`, dt2)
	}
	if d != "date" {
		t.Errorf(`want "date" got %q`, d)
	}
	if tm != "time" {
		t.Errorf(`want "time" got %q`, tm)
	}
}

func TestParameterValues(t *testing.T) {
	connector := newConnector(t)
	connector.RegisterExtendedType(Civil)

	pool := sql.OpenDB(connector)
	defer pool.Close()

	tin, err := time.Parse(time.RFC3339, "2006-01-02T22:04:05-07:00")
	if err != nil {
		t.Fatal(err)
	}

	var dt2, tm, d string
	err = pool.QueryRow(`
select
	dt2 = convert(nvarchar(200), @dt2, 121),
	d = convert(nvarchar(200), @d, 121),
	tm = convert(nvarchar(200), @tm, 121)
;
	`,
		sql.Named("dt2", civil.DateTimeOf(tin)),
		sql.Named("d", civil.DateOf(tin)),
		sql.Named("tm", civil.TimeOf(tin)),
	).Scan(&dt2, &d, &tm)
	if err != nil {
		t.Fatal(err)
	}
	if want := "2006-01-02 22:04:05.0000000"; dt2 != want {
		t.Errorf(`want %q got %q`, want, dt2)
	}
	if want := "2006-01-02"; d != want {
		t.Errorf(`want %q got %q`, want, d)
	}
	if want := "22:04:05.0000000"; tm != want {
		t.Errorf(`want %q got %q`, want, tm)
	}
}
