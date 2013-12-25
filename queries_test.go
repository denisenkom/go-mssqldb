package mssql

import (
    "testing"
    "time"
    "bytes"
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
        {"cast(1 as tinyint)", uint8(1)},
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
        {"cast(-0.5 as numeric(18,1))", Decimal{[...]uint32{5, 0, 0, 0}, false, 18, 1}},
        {"N'abc'", string("abc")},
        {"cast(null as nvarchar(3))", nil},
        {"NULL", nil},
        {"cast('2000-01-01' as datetime)", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
        {"cast('2000-01-01T12:13:14.12' as datetime)",
         time.Date(2000, 1, 1, 12, 13, 14, 120000000, time.UTC)},
        {"cast(NULL as datetime)", nil},
        {"cast('2000-01-01T12:13:00' as smalldatetime)",
         time.Date(2000, 1, 1, 12, 13, 0, 0, time.UTC)},
        {"cast('2000-01-01' as date)",
         time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
        {"cast(NULL as date)", nil},
        {"cast(0x6F9619FF8B86D011B42D00C04FC964FF as uniqueidentifier)",
         [...]byte{0x6F, 0x96, 0x19, 0xFF, 0x8B, 0x86, 0xD0, 0x11, 0xB4, 0x2D, 0x00, 0xC0, 0x4F, 0xC9, 0x64, 0xFF}},
        {"cast(NULL as uniqueidentifier)", nil},
        {"cast('00:00:45.123' as time(3))",
         time.Date(1, 1, 1, 00, 00, 45, 123000000, time.UTC) },
        {"cast('11:56:45.123' as time(3))",
         time.Date(1, 1, 1, 11, 56, 45, 123000000, time.UTC) },
        {"cast('2010-11-15T11:56:45.123' as datetime2(3))",
         time.Date(2010, 11, 15, 11, 56, 45, 123000000, time.UTC) },
        //{"cast('2010-11-15T11:56:45.123+10:00' as datetimeoffset(3))",
        // time.Date(2010, 11, 15, 11, 56, 45, 123000000, time.FixedZone("", 10*60*60)) },
        {"cast(0x1234 as varbinary(2))", []byte{0x12, 0x34}},
        {"cast(N'abc' as nvarchar(max))", "abc"},
        {"cast(null as nvarchar(max))", nil},
        //{"cast('<root></root>' as xml)", nil},
        {"cast('abc' as text)", "abc"},
        {"cast(null as text)", nil},
        {"cast(N'abc' as ntext)", "abc"},
        {"cast(0x1234 as image)", []byte{0x12, 0x34}},
    }

    for _, test := range values {
        stmt, err := conn.Prepare("select " + test.sql)
        if err != nil {
            t.Error("Prepare failed:", test.sql, err.Error())
            return
        }
        defer stmt.Close()

        row := stmt.QueryRow()
        var retval interface{}
        err = row.Scan(&retval)
        if err != nil {
            t.Error("Scan failed:", test.sql, err.Error())
            return
        }
        var same bool
        switch decodedval := retval.(type) {
        case []byte:
            switch decodedvaltest := test.val.(type) {
            case []byte:
                same = bytes.Equal(decodedval, decodedvaltest)
            default:
                same = false
            }
        default:
            same = retval == test.val
        }
        if !same {
            t.Error("Values don't match", test.sql, retval, test.val)
            return
        }
    }
}


func TestTrans(t *testing.T) {
    conn := open(t)
    defer conn.Close()

    tx, err := conn.Begin(); if err != nil {
        t.Error("Begin failed", err.Error())
        return
    }
    err = tx.Commit(); if err != nil {
        t.Error("Commit failed", err.Error())
        return
    }

    tx, err = conn.Begin(); if err != nil {
        t.Error("Begin failed", err.Error())
        return
    }
    err = tx.Rollback(); if err != nil {
        t.Error("Rollback failed", err.Error())
        return
    }
}


func TestParams(t *testing.T) {
    values := []interface{}{
        int64(5),
        "hello",
        []byte{1,2,3},
        //float32(1.2),
        float64(1.12313554),
        true,
        false,
    }

    conn := open(t)
    defer conn.Close()

    for _, val := range values {
        row := conn.QueryRow("select @p1", val)
        var retval interface{}
        err := row.Scan(&retval)
        if err != nil {
            t.Error("Scan failed", err.Error())
            return
        }
        var same bool
        switch decodedval := retval.(type) {
        case []byte:
            switch decodedvaltest := val.(type) {
            case []byte:
                same = bytes.Equal(decodedval, decodedvaltest)
            default:
                same = false
            }
        default:
            same = retval == val
        }
        if !same {
            t.Error("Value don't match", retval, val)
            return
        }
    }
}
