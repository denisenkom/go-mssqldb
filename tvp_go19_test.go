// +build go1.9

package mssql

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
)

func TestTVP(t *testing.T) {
	// TODO: Test default values

	checkConnStr(t)
	SetLogger(testLogger{t})

	db, err := sql.Open("sqlserver", makeConnStr(t).String())
	if err != nil {
		t.Fatalf("failed to open driver sqlserver")
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqltextcreatetable := `
		CREATE TYPE tvptable AS TABLE
		(
			p_binary            BINARY(3),
			p_varchar           VARCHAR(500),
			p_nvarchar          NVARCHAR(100),
			p_nvarchar1         NVARCHAR(100),
			p_id                UNIQUEIDENTIFIER,
			p_varbinary         VARBINARY(MAX),
			p_tinyint           TINYINT,
			p_smallint          SMALLINT,
			p_int               INT,
			p_bigint            BIGINT,
			p_bit               BIT,
			p_bit1              BIT,
			p_float             FLOAT
-- 			p_time 				datetime2
		); `

	sqltextdroptable := `DROP TYPE tvptable;`

	sqltextcreatesp := `
	CREATE PROCEDURE spwithtvp
		@param1 tvptable READONLY,
		@param2 tvptable READONLY,
		@param3 NVARCHAR(10)
	AS   
	BEGIN
		SET NOCOUNT ON; 
		SELECT * FROM @param1;
		SELECT * FROM @param2;
		SELECT @param3;
	END;`

	sqltextdropsp := `DROP PROCEDURE spwithtvp;`

	db.ExecContext(ctx, sqltextdropsp)
	db.ExecContext(ctx, sqltextdroptable)

	_, err = db.ExecContext(ctx, sqltextcreatetable)
	if err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, sqltextdroptable)

	_, err = db.ExecContext(ctx, sqltextcreatesp)
	if err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, sqltextdropsp)

	boolNull := true
	boolNull1 := false
	str := "bbb"
	param1 := []TvptableRow{
		TvptableRow{
			PBit:  &boolNull,
			PBit1: true,
			//Time:    time.Now().UTC(),
			PBigint: int64(64),
			PFloat:  float64(640),
			PInt:    int32(32),
		},
		TvptableRow{
			PBit:  &boolNull1,
			PBit1: false,
		},
		TvptableRow{
			PBinary:    nil,
			PVarchar:   "aaa",
			PNvarchar1: &str,
			PID:        UniqueIdentifier{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF},
			PVarbinary: []byte("ddd"),
			PTinyint:   1,
			PSmallint:  2,
			PInt:       3,
			PBigint:    4,
			PBit:       &boolNull,
			PFloat:     0.123,
		},
	}

	tvpType := TVPType{
		TVPName:  "tvptable",
		TVPValue: param1,
	}
	//tvpTypeEmpty := TVPType{
	//	TVPName:  "tvptable",
	//	TVPValue: []TvptableRow{TvptableRow{
	//		PBit:  &boolNull1,
	//		PBit1: false,
	//	}},
	//}

	rows, err := db.QueryContext(ctx,
		"exec spwithtvp @param1, @param2, @param3",
		sql.Named("param1", tvpType),
		sql.Named("param2", nil),
		sql.Named("param3", "test"),
	)

	if err != nil {
		t.Fatal(err)
	}

	var result1 []TvptableRow
	for rows.Next() {
		var val TvptableRow
		err := rows.Scan(&val.PBinary, &val.PVarchar, &val.PNvarchar, &val.PNvarchar1, &val.PID, &val.PVarbinary, &val.PTinyint, &val.PSmallint, &val.PInt, &val.PBigint, &val.PBit, &val.PBit1, &val.PFloat)
		if err != nil {
			t.Fatalf("scan failed with error: %s", err)
		}

		result1 = append(result1, val)
	}

	if !reflect.DeepEqual(param1, result1) {
		t.Logf("expected: %+v", param1)
		t.Logf("actual: %+v", result1)
		t.Errorf("first resultset did not match param1")
	}

	if !rows.NextResultSet() {
		t.Errorf("second resultset did not exist")
	}

	if rows.Next() {
		t.Errorf("second resultset was not empty")
	}

	if !rows.NextResultSet() {
		t.Errorf("third resultset did not exist")
	}

	if !rows.Next() {
		t.Errorf("third resultset was empty")
	}

	var result3 string
	if err := rows.Scan(&result3); err != nil {
		t.Errorf("error scanning third result set: %s", err)
	}
	if result3 != "test" {
		t.Errorf("third result set had wrong value expected: %s actual: %s", "test", result3)
	}
}

type Tvptable []TvptableRow

func (t *Tvptable) TVP() (typeName string, exampleRow []interface{}, rows [][]interface{}) {
	typeName = "tvptable"
	//columnNames = []string{ "p_binary", "p_varchar", "p_nvarchar", "p_id", "p_varbinary", "p_tinyint", "p_smallint", "p_int", "p_bigint", "p_bit", "p_float" }
	var v []TvptableRow
	if t != nil {
		v = *t
	}
	for _, r := range append(v, TvptableRow{}) {
		rows = append(rows, []interface{}{
			//r.PBinary,
			//r.PVarchar,
			r.PNvarchar,
			//r.PID,
			//r.PVarbinary,
			//r.PTinyint,
			//r.PSmallint,
			//r.PInt,
			//r.PBigint,
			//r.PBit,
			//r.PBit1,
			//r.PFloat,
		})
	}
	exampleRow = rows[len(rows)-1]
	rows = rows[:len(rows)-1]

	return
}

type TvptableRow struct {
	PBinary    []byte           `db:"p_binary"`
	PVarchar   string           `db:"p_varchar"`
	PNvarchar  string           `db:"p_nvarchar"`
	PNvarchar1 *string          `db:"p_nvarchar1"`
	PID        UniqueIdentifier `db:"p_id"`
	PVarbinary []byte           `db:"p_varbinary"`
	PTinyint   int8             `db:"p_tinyint"`
	PSmallint  int16            `db:"p_smallint"`
	PInt       int32            `db:"p_int"`
	PBigint    int64            `db:"p_bigint"`
	PBit       *bool            `db:"p_bit"`
	PBit1      bool             `db:"p_bit"`
	PFloat     float64          `db:"p_float"`
}
