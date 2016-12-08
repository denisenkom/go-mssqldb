// +build go1.8

package mssql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

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
	// calling next again should still return false
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
	if rows.NextResultSet() {
		t.Fatal("NextResultSet should return false but returned true")
	}
}

func TestColumnTypeIntrospection(t *testing.T) {
	type tst struct {
		expr         string
		typeName     string
		reflType     reflect.Type
		hasSize      bool
		size         int64
		hasPrecScale bool
		precision    int64
		scale        int64
	}
	tests := []tst{
		{"cast(1 as bit)", "BIT", reflect.TypeOf(true), false, 0, false, 0, 0},
		{"cast(1 as tinyint)", "TINYINT", reflect.TypeOf(int64(0)), false, 0, false, 0, 0},
		{"cast(1 as smallint)", "SMALLINT", reflect.TypeOf(int64(0)), false, 0, false, 0, 0},
		{"1", "INT", reflect.TypeOf(int64(0)), false, 0, false, 0, 0},
		{"cast(1 as bigint)", "BIGINT", reflect.TypeOf(int64(0)), false, 0, false, 0, 0},
		{"cast(1 as real)", "REAL", reflect.TypeOf(0.0), false, 0, false, 0, 0},
		{"cast(1 as float)", "FLOAT", reflect.TypeOf(0.0), false, 0, false, 0, 0},
		{"cast('abc' as varbinary(3))", "VARBINARY", reflect.TypeOf([]byte{}), true, 3, false, 0, 0},
		{"cast('abc' as varbinary(max))", "VARBINARY", reflect.TypeOf([]byte{}), true, 2147483645, false, 0, 0},
		{"cast(1 as datetime)", "DATETIME", reflect.TypeOf(time.Time{}), false, 0, false, 0, 0},
		{"cast(1 as smalldatetime)", "SMALLDATETIME", reflect.TypeOf(time.Time{}), false, 0, false, 0, 0},
		{"cast(getdate() as datetime2(7))", "DATETIME2", reflect.TypeOf(time.Time{}), false, 0, false, 0, 0},
		{"cast(getdate() as datetimeoffset(7))", "DATETIMEOFFSET", reflect.TypeOf(time.Time{}), false, 0, false, 0, 0},
		{"cast(getdate() as date)", "DATE", reflect.TypeOf(time.Time{}), false, 0, false, 0, 0},
		{"cast(getdate() as time)", "TIME", reflect.TypeOf(time.Time{}), false, 0, false, 0, 0},
		{"'abc'", "VARCHAR", reflect.TypeOf(""), true, 3, false, 0, 0},
		{"cast('abc' as varchar(max))", "VARCHAR", reflect.TypeOf(""), true, 2147483645, false, 0, 0},
		{"N'abc'", "NVARCHAR", reflect.TypeOf(""), true, 3, false, 0, 0},
		{"cast(N'abc' as NVARCHAR(MAX))", "NVARCHAR", reflect.TypeOf(""), true, 1073741822, false, 0, 0},
		{"cast(1 as decimal)", "DECIMAL", reflect.TypeOf([]byte{}), false, 0, true, 18, 0},
		{"cast(1 as decimal(5, 2))", "DECIMAL", reflect.TypeOf([]byte{}), false, 0, true, 5, 2},
		{"cast(1 as numeric(10, 4))", "DECIMAL", reflect.TypeOf([]byte{}), false, 0, true, 10, 4},
		{"cast(1 as money)", "MONEY", reflect.TypeOf([]byte{}), false, 0, false, 0, 0},
		{"cast(1 as smallmoney)", "SMALLMONEY", reflect.TypeOf([]byte{}), false, 0, false, 0, 0},
		{"cast(0x6F9619FF8B86D011B42D00C04FC964FF as uniqueidentifier)", "UNIQUEIDENTIFIER", reflect.TypeOf([]byte{}), false, 0, false, 0, 0},
		{"cast('<root/>' as xml)", "XML", reflect.TypeOf(""), true, 1073741822, false, 0, 0},
		{"cast('abc' as text)", "TEXT", reflect.TypeOf(""), true, 2147483647, false, 0, 0},
		{"cast(N'abc' as ntext)", "NTEXT", reflect.TypeOf(""), true, 1073741823, false, 0, 0},
		{"cast('abc' as image)", "IMAGE", reflect.TypeOf([]byte{}), true, 2147483647, false, 0, 0},
		{"cast('abc' as char(3))", "CHAR", reflect.TypeOf(""), true, 3, false, 0, 0},
		{"cast(N'abc' as nchar(3))", "NCHAR", reflect.TypeOf(""), true, 3, false, 0, 0},
		{"cast(1 as sql_variant)", "SQL_VARIANT", reflect.TypeOf(nil), false, 0, false, 0, 0},
	}
	conn := open(t)
	defer conn.Close()
	for _, tt := range tests {
		rows, err := conn.Query("select " + tt.expr)
		if err != nil {
			t.Errorf("Query failed with unexpected error %s", err)
		}
		ct, err := rows.ColumnTypes()
		if err != nil {
			t.Errorf("Query failed with unexpected error %s", err)
		}
		if ct[0].DatabaseTypeName() != tt.typeName {
			t.Errorf("Expected type %s but returned %s", tt.typeName, ct[0].DatabaseTypeName())
		}
		size, ok := ct[0].Length()
		if ok != tt.hasSize {
			t.Errorf("Expected has size %v but returned %v for %s", tt.hasSize, ok, tt.expr)
		} else {
			if ok && size != tt.size {
				t.Errorf("Expected size %d but returned %d for %s", tt.size, size, tt.expr)
			}
		}

		prec, scale, ok := ct[0].DecimalSize()
		if ok != tt.hasPrecScale {
			t.Errorf("Expected has prec/scale %v but returned %v for %s", tt.hasPrecScale, ok, tt.expr)
		} else {
			if ok && prec != tt.precision {
				t.Errorf("Expected precision %d but returned %d for %s", tt.precision, prec, tt.expr)
			}
			if ok && scale != tt.scale {
				t.Errorf("Expected scale %d but returned %d for %s", tt.scale, scale, tt.expr)
			}
		}

		if ct[0].ScanType() != tt.reflType {
			t.Errorf("Expected ScanType %v but got %v for %s", tt.reflType, ct[0].ScanType(), tt.expr)
		}
	}
}

func TestColumnIntrospection(t *testing.T) {
	type tst struct {
		expr         string
		fieldName    string
		typeName     string
		nullable     bool
		hasSize      bool
		size         int64
		hasPrecScale bool
		precision    int64
		scale        int64
	}
	tests := []tst{
		{"f1 int null", "f1", "INT", true, false, 0, false, 0, 0},
		{"f2 varchar(15) not null", "f2", "VARCHAR", false, true, 15, false, 0, 0},
		{"f3 decimal(5, 2) null", "f3", "DECIMAL", true, false, 0, true, 5, 2},
	}
	conn := open(t)
	defer conn.Close()

	// making table variable with specified fields and making a select from it
	exprs := make([]string, len(tests))
	for i, test := range tests {
		exprs[i] = test.expr
	}
	exprJoined := strings.Join(exprs, ",")
	rows, err := conn.Query(fmt.Sprintf("declare @tbl table(%s); select * from @tbl", exprJoined))
	if err != nil {
		t.Errorf("Query failed with unexpected error %s", err)
	}

	ct, err := rows.ColumnTypes()
	if err != nil {
		t.Errorf("ColumnTypes failed with unexpected error %s", err)
	}
	for i, test := range tests {
		if ct[i].Name() != test.fieldName {
			t.Errorf("Field expected have name %s but it has name %s", test.fieldName, ct[i].Name())
		}

		if ct[i].DatabaseTypeName() != test.typeName {
			t.Errorf("Invalid type name returned %s expected %s", ct[i].DatabaseTypeName(), test.typeName)
		}

		nullable, ok := ct[i].Nullable()
		if ok {
			if nullable != test.nullable {
				t.Errorf("Invalid nullable value returned %v", nullable)
			}
		} else {
			t.Error("Nullable was expected to support Nullable but it didn't")
		}

		size, ok := ct[i].Length()
		if ok != test.hasSize {
			t.Errorf("Expected has size %v but returned %v for %s", test.hasSize, ok, test.expr)
		} else {
			if ok && size != test.size {
				t.Errorf("Expected size %d but returned %d for %s", test.size, size, test.expr)
			}
		}

		prec, scale, ok := ct[i].DecimalSize()
		if ok != test.hasPrecScale {
			t.Errorf("Expected has prec/scale %v but returned %v for %s", test.hasPrecScale, ok, test.expr)
		} else {
			if ok && prec != test.precision {
				t.Errorf("Expected precision %d but returned %d for %s", test.precision, prec, test.expr)
			}
			if ok && scale != test.scale {
				t.Errorf("Expected scale %d but returned %d for %s", test.scale, scale, test.expr)
			}
		}
	}
}

func TestContext(t *testing.T) {
	conn := open(t)
	defer conn.Close()
	ctx := context.Background()
	ctx = sql.IsolationContext(ctx, sql.LevelSerializable)
	tx, err := conn.BeginContext(ctx)
	if err != nil {
		t.Errorf("BeginContext failed with unexpected error %s", err)
		return
	}
	rows, err := tx.QueryContext(ctx, "DBCC USEROPTIONS")
	properties := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err = rows.Scan(&name, &value); err != nil {
			t.Errorf("Scan failed with unexpected error %s", err)
		}
		properties[name] = value
	}

	if properties["isolation level"] != "serializable" {
		t.Errorf("Expected isolation level to be serializable but it is %s", properties["isolation level"])
	}

	row := tx.QueryRowContext(ctx, "select 1")
	var val int64
	if err = row.Scan(&val); err != nil {
		t.Errorf("QueryRowContext failed with unexpected error %s", err)
	}
	if val != 1 {
		t.Error("Incorrect value returned from query")
	}

	_, err = tx.ExecContext(ctx, "select 1")
	if err != nil {
		t.Errorf("ExecContext failed with unexpected error %s", err)
		return
	}

	_, err = tx.PrepareContext(ctx, "select 1")
	if err != nil {
		t.Errorf("PrepareContext failed with unexpected error %s", err)
		return
	}
}

func TestNamedParameters(t *testing.T) {
	conn := open(t)
	defer conn.Close()
	row := conn.QueryRow(
		"select :param2, :param1, :param2",
		sql.Named("param1", 1),
		sql.Named("param2", 2))
	var col1, col2, col3 int64
	err := row.Scan(&col1, &col2, &col3)
	if err != nil {
		t.Errorf("Scan failed with unexpected error %s", err)
		return
	}
	if col1 != 2 || col2 != 1 || col3 != 2 {
		t.Errorf("Unexpected values returned col1=%d, col2=%d, col3=%d", col1, col2, col3)
	}
}

func TestBadNamedParameters(t *testing.T) {
	conn := open(t)
	defer conn.Close()
	row := conn.QueryRow(
		"select :param2, :param1, :param2",
		sql.Named("badparam1", 1),
		sql.Named("param2", 2))
	var col1, col2, col3 int64
	err := row.Scan(&col1, &col2, &col3)
	if err == nil {
		t.Error("Scan succeeded unexpectedly")
		return
	}
	t.Logf("Scan failed as expected with error %s", err)
}

func TestMixedParameters(t *testing.T) {
	conn := open(t)
	defer conn.Close()
	row := conn.QueryRow(
		"select :2, :param1, :param2",
		5, // this parameter will be unused
		6,
		sql.Named("param1", 1),
		sql.Named("param2", 2))
	var col1, col2, col3 int64
	err := row.Scan(&col1, &col2, &col3)
	if err != nil {
		t.Errorf("Scan failed with unexpected error %s", err)
		return
	}
	if col1 != 6 || col2 != 1 || col3 != 2 {
		t.Errorf("Unexpected values returned col1=%d, col2=%d, col3=%d", col1, col2, col3)
	}
}

func TestPinger(t *testing.T) {
	conn := open(t)
	defer conn.Close()
	err := conn.Ping()
	if err != nil {
		t.Errorf("Failed to hit database")
	}
}

func TestQueryCancelLowLevel(t *testing.T) {
	drv := &MssqlDriver{}
	conn, err := drv.open(makeConnStr())
	if err != nil {
		t.Errorf("Open failed with error %v", err)
	}

	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	stmt, err := conn.prepareContext(ctx, "waitfor delay '00:00:03'")
	if err != nil {
		t.Errorf("Prepare failed with error %v", err)
	}
	err = stmt.sendQuery([]namedValue{})
	if err != nil {
		t.Errorf("sendQuery failed with error %v", err)
	}

	cancel()

	_, err = stmt.processExec(ctx)
	if err != context.Canceled {
		t.Errorf("Expected error to be Cancelled but got %v", err)
	}

	// same connection should be usable again after it was cancelled
	stmt, err = conn.prepareContext(context.Background(), "select 1")
	if err != nil {
		t.Errorf("Prepare failed with error %v", err)
	}
	rows, err := stmt.Query([]driver.Value{})
	if err != nil {
		t.Errorf("Query failed with error %v", err)
	}

	values := []driver.Value{nil}
	err = rows.Next(values)
	if err != nil {
		t.Errorf("Next failed with error %v", err)
	}
}

func TestQueryCancelHighLevel(t *testing.T) {
	conn := open(t)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(1 * time.Second)
		cancel()
	}()
	_, err := conn.ExecContext(ctx, "waitfor delay '00:00:03'")
	if err != context.Canceled {
		t.Errorf("ExecContext expected to fail with Cancelled but it returned %v", err)
	}

	// connection should be usable after timeout
	row := conn.QueryRow("select 1")
	var val int64
	err = row.Scan(&val)
	if err != nil {
		t.Fatal("Scan failed with", err)
	}
}

func TestQueryTimeout(t *testing.T) {
	conn := open(t)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Second)
	defer cancel()
	_, err := conn.ExecContext(ctx, "waitfor delay '00:00:03'")
	if err != context.DeadlineExceeded {
		t.Errorf("ExecContext expected to fail with DeadlineExceeded but it returned %v", err)
	}

	// connection should be usable after timeout
	row := conn.QueryRow("select 1")
	var val int64
	err = row.Scan(&val)
	if err != nil {
		t.Fatal("Scan failed with", err)
	}
}
