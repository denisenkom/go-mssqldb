package mssql

import (
    "testing"
)

func TestSelect(t *testing.T) {
    conn := open(t)
    defer conn.Close()

    type testStruct struct {
        sql string
        val interface{}
    }

    values := []testStruct{
        {"1", int32(1)},
        {"'abc'", string("abc")},
    }

    for _, test := range values {
        stmt, err := conn.Prepare("select " + test.sql)
        if err != nil {
            t.Error("Prepare failed:", err.Error())
            return
        }
        defer stmt.Close()

        row := stmt.QueryRow()
        var retval interface{}
        err = row.Scan(&retval)
        if err != nil {
            t.Error("Scan failed:", err.Error())
            return
        }
        if retval != test.val {
            t.Error("Values don't match", retval, test.val)
            return
        }
    }
}
