// +build go1.9

package mssql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestBulkcopy(t *testing.T) {
	// TDS level Bulk Insert is not supported on Azure SQL Server.
	if dsn := makeConnStr(t); strings.HasSuffix(strings.Split(dsn.Host, ":")[0], ".database.windows.net") {
		t.Skip("TDS level bulk copy is not supported on Azure SQL Server")
	}
	type testValue struct {
		colname string
		in, out interface{}
	}

	tableName := "#table_test"
	geom, _ := hex.DecodeString("E6100000010C00000000000034400000000000004440")
	bin, _ := hex.DecodeString("ba8b7782168d4033a299333aec17bd33")
	uid := []byte{0x6F, 0x96, 0x19, 0xFF, 0x8B, 0x86, 0xD0, 0x11, 0xB4, 0x2D, 0x00, 0xC0, 0x4F, 0xC9, 0x64, 0xFF}
	testValues := []testValue{
		{"test_nvarchar", "ab©ĎéⒻghïjklmnopqЯ☀tuvwxyz", nil},
		{"test_nvarchar_max", "ab©ĎéⒻghïjklmnopqЯ☀tuvwxyz", nil},
		{"test_nvarchar_max_nil", nil, nil},
		{"test_varchar", "abcdefg", nil},
		{"test_varchar_max", "abcdefg", nil},
		{"test_varchar_max_nil", nil, nil},
		{"test_char", "abcdefg   ", nil},
		{"test_nchar", "abcdefg   ", nil},
		{"test_text", "abcdefg", nil},
		{"test_ntext", "abcdefg", nil},
		{"test_float", 1234.56, nil},
		{"test_floatn", 1234.56, nil},
		{"test_real", 1234.56, nil},
		{"test_realn", 1234.56, nil},
		{"test_bit", true, nil},
		{"test_bitn", nil, nil},
		{"test_smalldatetime", time.Date(2010, 11, 12, 13, 14, 0, 0, time.UTC), nil},
		{"test_smalldatetimen", time.Date(2010, 11, 12, 13, 14, 0, 0, time.UTC), nil},
		{"test_datetime", time.Date(2010, 11, 12, 13, 14, 15, 120000000, time.UTC), nil},
		{"test_datetimen", time.Date(2010, 11, 12, 13, 14, 15, 120000000, time.UTC), nil},
		{"test_datetimen_1", time.Date(4010, 11, 12, 13, 14, 15, 120000000, time.UTC), nil},
		{"test_datetime2_1", "2010-11-12 13:14:15Z", time.Date(2010, 11, 12, 13, 14, 15, 0, time.UTC)},
		{"test_datetime2_3", time.Date(2010, 11, 12, 13, 14, 15, 123000000, time.UTC), nil},
		{"test_datetime2_7", time.Date(2010, 11, 12, 13, 14, 15, 123000000, time.UTC), nil},
		{"test_datetimeoffset_7", time.Date(2010, 11, 12, 13, 14, 15, 123000000, time.UTC), nil},
		{"test_date", time.Date(2010, 11, 12, 0, 0, 0, 0, time.UTC), nil},
		{"test_date_2", "2015-06-07", time.Date(2015, 6, 7, 0, 0, 0, 0, time.UTC)},
		{"test_time", time.Date(2010, 11, 12, 13, 14, 15, 123000000, time.UTC), time.Date(1, 1, 1, 13, 14, 15, 123000000, time.UTC)},
		{"test_time_2", "13:14:15.1230000", time.Date(1, 1, 1, 13, 14, 15, 123000000, time.UTC)},
		{"test_tinyint", 255, nil},
		{"test_smallint", 32767, nil},
		{"test_smallintn", nil, nil},
		{"test_int", 2147483647, nil},
		{"test_bigint", 9223372036854775807, nil},
		{"test_bigintn", nil, nil},
		{"test_intf", 1234.56, 1234},
		{"test_intf32", float32(1234.56), 1234},
		{"test_geom", geom, string(geom)},
		{"test_uniqueidentifier", uid, string(uid)},
		// {"test_smallmoney", 1234.56, nil},
		// {"test_money", 1234.56, nil},
		{"test_decimal_18_0", 1234.0001, "1234"},
		{"test_decimal_9_2", -1234.560001, "-1234.56"},
		{"test_decimal_20_0", 1234, "1234"},
		{"test_decimal_20_0_2", math.MinInt64, "-9223372036854775808"},
		{"test_decimal_20_10", "1234.1", "1234.1000000000"},
		{"test_numeric_30_10", "66666666666666666666.6666666666", nil},
		{"test_varbinary", []byte("1"), nil},
		{"test_varbinary_16", bin, nil},
		{"test_varbinary_max", bin, nil},
		{"test_binary", []byte("1"), nil},
		{"test_binary_16", bin, nil},
		{"test_intvarchar", 1234, "1234"},
		{"test_intnvarchar", 1234, "1234"},
	}

	columns := make([]string, len(testValues))
	for i, val := range testValues {
		columns[i] = val.colname
	}

	values := make([]interface{}, len(testValues))
	for i, val := range testValues {
		values[i] = val.in
	}

	pool, logger := open(t)
	defer pool.Close()
	defer logger.StopLogging()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Now that session resetting is supported, the use of the per session
	// temp table requires the use of a dedicated connection from the connection
	// pool.
	conn, err := pool.Conn(ctx)
	if err != nil {
		t.Fatal("failed to pull connection from pool", err)
	}
	defer conn.Close()

	err = setupTable(ctx, t, conn, tableName)
	if err != nil {
		t.Error("Setup table failed: ", err)
		return
	}

	t.Log("Preparing copy in statement")

	stmt, err := conn.PrepareContext(ctx, CopyIn(tableName, BulkOptions{}, columns...))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for i := 0; i < 10; i++ {
		t.Logf("Executing copy in statement %d time with %d values", i+1, len(values))
		_, err = stmt.Exec(values...)
		if err != nil {
			t.Error("AddRow failed: ", err.Error())
			return
		}
	}

	result, err := stmt.Exec()
	if err != nil {
		t.Fatal("bulkcopy failed: ", err.Error())
	}

	insertedRowCount, _ := result.RowsAffected()
	if insertedRowCount == 0 {
		t.Fatal("0 row inserted!")
	}

	//check that all rows are present
	var rowCount int
	err = conn.QueryRowContext(ctx, "select count(*) c from "+tableName).Scan(&rowCount)
	if err != nil {
		t.Fatal(err)
	}

	if rowCount != 10 {
		t.Errorf("unexpected row count %d", rowCount)
	}

	//data verification
	rows, err := conn.QueryContext(ctx, "select "+strings.Join(columns, ",")+" from "+tableName)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {

		ptrs := make([]interface{}, len(columns))
		container := make([]interface{}, len(columns))
		for i := range ptrs {
			ptrs[i] = &container[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		for i, c := range testValues {
			expected := c.out
			if expected == nil {
				expected = c.in
			}
			if !compareValue(container[i], expected) {
				v := container[i]
				if s, ok := v.([]uint8); ok {
					v = string(s)
				}
				t.Errorf("columns %s : expected: %T %v, got: %T %v\n", c.colname, expected, expected, container[i], v)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Error(err)
	}
}

func compareValue(a interface{}, expected interface{}) bool {
	if got, ok := a.([]uint8); ok {
		if _, ok := expected.([]uint8); !ok {
			a = string(got)
		}
	}

	switch expected := expected.(type) {
	case int:
		return int64(expected) == a
	case int32:
		return int64(expected) == a
	case int64:
		return expected == a
	case float64:
		return math.Abs(expected-a.(float64)) < 0.0001
	case time.Time:
		if got, ok := a.(time.Time); ok {
			_, ez := expected.Zone()
			_, az := got.Zone()
			return expected.Equal(got) && ez == az
		}
		return false
	default:
		return reflect.DeepEqual(expected, a)
	}
}

func setupTable(ctx context.Context, t *testing.T, conn *sql.Conn, tableName string) (err error) {
	tablesql := `CREATE TABLE ` + tableName + ` (
	[id] [int] IDENTITY(1,1) NOT NULL,
	[test_nvarchar] [nvarchar](50) NULL,
	[test_nvarchar_4000] [nvarchar](4000) NULL,
	[test_nvarchar_max] [nvarchar](max) NULL,
	[test_nvarchar_max_nil] [nvarchar](max) NULL,
	[test_varchar] [varchar](50) NULL,
	[test_varchar_8000] [varchar](8000) NULL,
	[test_varchar_max] [varchar](max) NULL,
	[test_varchar_max_nil] [varchar](max) NULL,
	[test_char] [char](10) NULL,
	[test_nchar] [nchar](10) NULL,
	[test_text] [text] NULL,
	[test_ntext] [ntext] NULL,
	[test_float] [float] NOT NULL,
	[test_floatn] [float] NULL,
	[test_real] [real] NULL,
	[test_realn] [real] NULL,
	[test_bit] [bit] NOT NULL,
	[test_bitn] [bit] NULL,
	[test_smalldatetime] [smalldatetime] NOT NULL,
	[test_smalldatetimen] [smalldatetime] NULL,
	[test_datetime] [datetime] NOT NULL,
	[test_datetimen] [datetime] NULL,
	[test_datetimen_1] [datetime] NULL,
	[test_datetime2_1] [datetime2](1) NULL,
	[test_datetime2_3] [datetime2](3) NULL,
	[test_datetime2_7] [datetime2](7) NULL,
	[test_datetimeoffset_7] [datetimeoffset](7) NULL,
	[test_date] [date] NULL,
	[test_date_2] [date] NULL,
	[test_time] [time](7) NULL,
	[test_time_2] [time](7) NULL,
	[test_smallmoney] [smallmoney] NULL,
	[test_money] [money] NULL,
	[test_tinyint] [tinyint] NULL,
	[test_smallint] [smallint] NOT NULL,
	[test_smallintn] [smallint] NULL,
	[test_int] [int] NULL,
	[test_bigint] [bigint] NOT NULL,
	[test_bigintn] [bigint] NULL,
	[test_intf] [int] NULL,
	[test_intf32] [int] NULL,
	[test_geom] [geometry] NULL,
	[test_geog] [geography] NULL,
	[text_xml] [xml] NULL,
	[test_uniqueidentifier] [uniqueidentifier] NULL,
	[test_decimal_18_0] [decimal](18, 0) NULL,
	[test_decimal_18_2] [decimal](18, 2) NULL,
	[test_decimal_9_2] [decimal](9, 2) NULL,
	[test_decimal_20_0] [decimal](20, 0) NULL,
	[test_decimal_20_0_2] [decimal](20, 0) NULL,
	[test_decimal_20_10] [decimal](20, 10) NULL,
	[test_numeric_30_10] [decimal](30, 10) NULL,
	[test_varbinary] VARBINARY NOT NULL,
	[test_varbinary_16] VARBINARY(16) NOT NULL,
	[test_varbinary_max] VARBINARY(max) NOT NULL,
	[test_binary] BINARY NOT NULL,
	[test_binary_16] BINARY(16) NOT NULL,
	[test_intvarchar] [varchar](4) NULL,
	[test_intnvarchar] [varchar](4) NULL,
 CONSTRAINT [PK_` + tableName + `_id] PRIMARY KEY CLUSTERED
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];`
	_, err = conn.ExecContext(ctx, tablesql)
	if err != nil {
		t.Fatal("tablesql failed:", err)
	}
	return
}
