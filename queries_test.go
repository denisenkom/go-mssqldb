package mssql

import (
    "testing"
    "time"
    "bytes"
    "database/sql"
    "strings"
    "fmt"
)

func TestSelect(t *testing.T) {
    conn := open(t)
    defer conn.Close()

    type testStruct struct {
        sql string
        val interface{}
    }

    longstr := strings.Repeat("x", 10000)

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
        {"cast('<root/>' as xml)", "<root/>"},
        {"cast('abc' as text)", "abc"},
        {"cast(null as text)", nil},
        {"cast(N'abc' as ntext)", "abc"},
        {"cast(0x1234 as image)", []byte{0x12, 0x34}},
        {"cast(N'проверка' as nvarchar(max))", "проверка"},
        {fmt.Sprintf("cast(N'%s' as nvarchar(max))", longstr), longstr},
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

    var tx *sql.Tx
    var err error
    if tx, err = conn.Begin(); err != nil {
        t.Fatal("Begin failed", err.Error())
    }
    if err = tx.Commit(); err != nil {
        t.Fatal("Commit failed", err.Error())
    }

    if tx, err = conn.Begin(); err != nil {
        t.Fatal("Begin failed", err.Error())
    }
    if _, err = tx.Exec("create table ##abc (fld int)"); err != nil {
        t.Fatal("Create table failed", err.Error())
    }
    if err = tx.Rollback(); err != nil {
        t.Fatal("Rollback failed", err.Error())
    }
}


func TestParams(t *testing.T) {
    longstr := strings.Repeat("x", 10000)
    values := []interface{}{
        int64(5),
        "hello",
        []byte{1,2,3},
        //float32(1.2),
        float64(1.12313554),
        true,
        false,
        nil,
        longstr,
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


func TestExec(t *testing.T) {
    conn := open(t)
    defer conn.Close()

    res, err := conn.Exec("create table ##abc (fld int)")
    if err != nil {
        t.Fatal("Exec failed", err.Error())
    }
    _ = res
}


func TestTimeout(t *testing.T) {
    if testing.Short() {
        return
    }
    conn := open(t)
    defer conn.Close()

    res, err := conn.Exec("waitfor delay '00:31'")
    if err == nil {
        t.Fatal("Exec should fail with timeout")
    }
    if neterr, ok := err.(Error); !ok || !neterr.Timeout() {
        t.Fatal("Exec should fail with timeout, failed with", err)
    }
    _ = res
}


func TestTwoQueries(t *testing.T) {
    conn := open(t)
    defer conn.Close()

    rows, err := conn.Query("select 1")
    if err != nil {
        t.Fatal("First exec failed", err)
    }
    if !rows.Next() {
        t.Fatal("First query didn't return row")
    }
    var i int
    if err = rows.Scan(&i); err != nil {
        t.Fatal("Scan failed", err)
    }
    if i != 1 {
        t.Fatalf("Wrong value returned %d, should be 1", i)
    }

    if rows, err = conn.Query("select 2"); err != nil {
        t.Fatal("Second query failed", err)
    }
    if !rows.Next() {
        t.Fatal("Second query didn't return row")
    }
    if err = rows.Scan(&i); err != nil {
        t.Fatal("Scan failed", err)
    }
    if i != 2 {
        t.Fatalf("Wrong value returned %d, should be 2", i)
    }
}


func TestError(t *testing.T) {
    conn := open(t)
    defer conn.Close()

    _, err := conn.Query("exec bad")
    if err == nil {
        t.Fatal("Query should fail")
    }

    if err, ok := err.(Error); !ok {
        t.Fatalf("Should be sql error, actually %t, %v", err, err)
    } else {
        if err.Number != 2812 { // Could not find stored procedure 'bad'
            t.Fatalf("Should be specific error code 2812, actually %d %s", err.Number, err)
        }
    }
}


func TestQueryNoRows(t *testing.T) {
    conn := open(t)
    defer conn.Close()

    var rows *sql.Rows
    var err error
    if rows, err = conn.Query("create table ##abc (fld int)"); err != nil {
        t.Fatal("Query failed", err)
    }
    if rows.Next() {
        t.Fatal("Query shoulnd't return any rows")
    }
}


func TestQueryManyNullsRow(t *testing.T) {
    conn := open(t)
    defer conn.Close()

    var row *sql.Row
    var err error
    if row = conn.QueryRow("select null, null, null, null, null, null, null, null"); err != nil {
        t.Fatal("Query failed", err)
    }
    var v [8]sql.NullInt64
    if err = row.Scan(&v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7]); err != nil {
        t.Fatal("Scan failed", err)
    }
}
