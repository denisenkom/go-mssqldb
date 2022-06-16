//go:build go1.10
// +build go1.10

package mssql

import (
	"database/sql"
	"testing"
)

// The default value converter promotes every int type to bigint.
// This benchmark forces that mismatch for comparing the query performance with the
// fixed version of the driver that doesn't perform such promotion.
// It may not show much of a time difference. Look for the actual query via plan or xevents
// while the benchmark runs to make sure it's passing the correct int type.
func BenchmarkSelectWithTypeMismatch(b *testing.B) {
	connector, err := NewConnector(makeConnStr(b).String())
	if err != nil {
		b.Fatal("Open connection failed:", err.Error())
	}
	conn := sql.OpenDB(connector)
	defer conn.Close()
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	rows, err := conn.Query("select 'prime the pump'")
	if err != nil {
		b.Fatal("Unable to query")
	}
	rows.Close()
	if rows.Err() != nil {
		b.Fatal("Rows error:", rows.Err())
	}
	b.Run("PromoteToBigInt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, err := conn.Query(`SELECT Count(*) from sys.all_objects where object_id > @obid`, sql.Named("obid", int64(-605853368)))
			if err != nil {
				b.Fatal("Query failed:", err.Error())
			}
			defer rows.Close()
			for rows.Next() {
			}
			if rows.Err() != nil {
				b.Fatal("Rows error:", rows.Err())
			}
		}
	})
	b.Run("NoIntPromotion", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, err := conn.Query(`SELECT Count(*) from sys.all_objects where object_id > @obid`, sql.Named("obid", int32(-605853368)))
			if err != nil {
				b.Fatal("Query failed:", err.Error())
			}
			defer rows.Close()
			for rows.Next() {
			}
			if rows.Err() != nil {
				b.Fatal("Rows error:", rows.Err())
			}
		}
	})

}
