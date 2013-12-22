package mssql

import (
    "testing"
    "time"
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
        {"cast(1 as tinyint)", int8(1)},
        {"cast(1 as smallint)", int16(1)},
        {"cast(1 as bigint)", int64(1)},
        {"cast(1 as bit)", true},
        {"cast(0 as bit)", false},
        {"'abc'", string("abc")},
        {"cast(0.5 as float)", float64(0.5)},
        {"cast(0.5 as real)", float32(0.5)},
        {"cast(1 as decimal)", Decimal{[...]uint32{1, 0, 0, 0}, true, 18, 0}},
        {"cast(0.5 as decimal(18,1))", Decimal{[...]uint32{5, 0, 0, 0}, true, 18, 1}},
        {"cast(-0.5 as decimal(18,1))", Decimal{[...]uint32{5, 0, 0, 0}, false, 18, 1}},
        {"N'abc'", string("abc")},
        {"NULL", nil},
        {"cast('2000-01-01' as datetime)", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
        {"cast('2000-01-01T12:13:14.12' as datetime)",
         time.Date(2000, 1, 1, 12, 13, 14, 120000000, time.UTC)},
        {"cast(NULL as datetime)", nil},
        {"cast('2000-01-01T12:13:00' as smalldatetime)",
         time.Date(2000, 1, 1, 12, 13, 0, 0, time.UTC)},
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
