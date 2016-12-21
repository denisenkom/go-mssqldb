package mssql

import (
	"database/sql"
	"encoding/hex"
	"log"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestBulkcopy(t *testing.T) {

	type testValue struct {
		colname string
		val     interface{}
	}

	geom, _ := hex.DecodeString("E6100000010C00000000000034400000000000004440")
	testValues := []testValue{
		{"test_nvarchar", "ab©ĎéⒻghïjklmnopqЯ☀tuvwxyz"},
		{"test_varchar", "abcdefg"},
		{"test_float", 1234.56},
		{"test_smalldatetime", time.Date(2010, 11, 12, 13, 14, 0, 0, time.UTC)},
		{"test_datetime", time.Date(2010, 11, 12, 13, 14, 15, 120000000, time.UTC)},
		{"test_datetime2_3", time.Date(2010, 11, 12, 13, 14, 15, 123000000, time.UTC)},
		{"test_smallint", 234},
		{"test_bigint", 123123123},
		{"test_bit", true},
		{"test_geom", geom},
	}

	columns := make([]string, len(testValues))
	for i, val := range testValues {
		columns[i] = val.colname
	}

	values := make([]interface{}, len(testValues))
	for i, val := range testValues {
		values[i] = val.val
	}

	conn := open(t)
	defer conn.Close()

	setupTable(conn)

	stmt, err := conn.Prepare(CopyIn("#table_test", MssqlBulkOptions{}, columns...))

	for i := 0; i < 10; i++ {
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
	err = conn.QueryRow("select count(*) c from #table_test").Scan(&rowCount)

	if rowCount != 10 {
		t.Errorf("unexpected row count %d", rowCount)
	}

	//data verification
	rows, err := conn.Query("select " + strings.Join(columns, ",") + " from #table_test")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {

		ptrs := make([]interface{}, len(columns))
		container := make([]interface{}, len(columns))
		for i, _ := range ptrs {
			ptrs[i] = &container[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		for i, c := range testValues {
			if !compareValue(container[i], c.val) {
				t.Errorf("columns %s : %s != %s\n", c.colname, container[i], c.val)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Error(err)
	}
}

func compareValue(a interface{}, expected interface{}) bool {
	switch expected := expected.(type) {
	case int:
		return int64(expected) == a
	case int32:
		return int64(expected) == a
	case int64:
		return int64(expected) == a
	default:
		return reflect.DeepEqual(expected, a)
	}
}
func setupTable(conn *sql.DB) {

	tablesql := `CREATE TABLE #table_test(
	[id] [int] IDENTITY(1,1) NOT NULL,
	[test_nvarchar] [nvarchar](50) NULL,
	[test_varchar] [varchar](50) NULL,
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
	[test_datetime2_1] [datetime2](1) NULL,
	[test_datetime2_3] [datetime2](3) NULL,
	[test_datetime2_7] [datetime2](7) NULL,
	[test_date] [date] NULL,
	[test_smallmoney] [smallmoney] NULL,
	[test_money] [money] NULL,
	[test_tinyint] [tinyint] NULL,
	[test_smallint] [smallint] NOT NULL,
	[test_smallintn] [smallint] NULL,
	[test_int] [int] NULL,
	[test_bigint] [bigint] NOT NULL,
	[test_bigintn] [bigint] NULL,
	[test_geom] [geometry] NULL,
	[test_geog] [geography] NULL,
	[text_xml] [xml] NULL,
	[test_uniqueidentifier] [uniqueidentifier] NULL,
	[test_decimal_18_0] [decimal](18, 0) NULL,
	[test_decimal_9_2] [decimal](9, 2) NULL,
	[test_decimal_20_0] [decimal](20, 0) NULL,
 CONSTRAINT [PK_table_test_id2] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];`
	_, err := conn.Exec(tablesql)
	if err != nil {
		log.Fatal("tablesql failed:", err)
	}

}
