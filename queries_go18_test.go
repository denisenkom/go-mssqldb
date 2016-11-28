// +build go1.8

package mssql

import "testing"

func TestNextResultSet(t *testing.T) {
	conn := open(t)
	defer conn.Close()
	rows, err := conn.Query("select 1; select 2")
	if err != nil {
		t.Fatal("Query failed", err.Error())
	}
	defer func() {
		err := rows.Err()
		if err != nil {
			t.Error("unexpected error:", err)
		}
	}()

	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Query didn't return row")
	}
	var fld1, fld2 int32
	err = rows.Scan(&fld1)
	if err != nil {
		t.Fatal("Scan failed", err)
	}
	if fld1 != 1 {
		t.Fatal("Returned value doesn't match")
	}
	if rows.Next() {
		t.Fatal("Query returned unexpected second row.")
	}
	if !rows.NextResultSet() {
		t.Fatal("NextResultSet should return true but returned false")
	}
	if !rows.Next() {
		t.Fatal("Query didn't return row")
	}
	err = rows.Scan(&fld2)
	if err != nil {
		t.Fatal("Scan failed", err)
	}
	if fld2 != 2 {
		t.Fatal("Returned value doesn't match")
	}
}
