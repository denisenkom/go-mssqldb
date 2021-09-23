// +build go1.9

package mssql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/golang-sql/sqlexp"
)

func TestOutputParam(t *testing.T) {
	checkConnStr(t)
	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)

	db, err := sql.Open("sqlserver", makeConnStr(t).String())
	if err != nil {
		t.Fatalf("failed to open driver sqlserver")
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("sp with rows", func(t *testing.T) {
		sqltextcreate := `
CREATE PROCEDURE spwithrows
   @intparam INT = NULL OUTPUT
AS
BEGIN
   -- return 2 rows
   SELECT @intparam
   union
   SELECT 20

   -- set output parameter value
   SELECT @intparam = 10
END;
`
		sqltextdrop := `DROP PROCEDURE spwithrows;`
		sqltextrun := `spwithrows`

		db.ExecContext(ctx, sqltextdrop)
		_, err = db.ExecContext(ctx, sqltextcreate)
		if err != nil {
			t.Fatal(err)
		}
		defer db.ExecContext(ctx, sqltextdrop)
		if err != nil {
			t.Error(err)
		}

		var intparam int = 5
		rows, err := db.QueryContext(ctx, sqltextrun,
			sql.Named("intparam", sql.Out{Dest: &intparam}),
		)
		if err != nil {
			t.Error(err)
		}
		defer rows.Close()
		// reading first row
		if !rows.Next() {
			t.Error("Next returned false")
		}
		var rowval int
		err = rows.Scan(&rowval)
		if err != nil {
			t.Error(err)
		}
		if rowval != 5 {
			t.Errorf("expected 5, got %d", rowval)
		}

		// if uncommented would trigger race condition warning
		//if intparam != 10 {
		//	t.Log("output parameter value is not yet 10, it is ", intparam)
		//}

		// reading second row
		if !rows.Next() {
			t.Error("Next returned false")
		}
		err = rows.Scan(&rowval)
		if err != nil {
			t.Error(err)
		}
		if rowval != 20 {
			t.Errorf("expected 20, got %d", rowval)
		}

		if rows.Next() {
			t.Error("Next returned true but should return false after last row was returned")
		}

		if intparam != 10 {
			t.Errorf("expected 10, got %d", intparam)
		}
	})

	t.Run("sp with no rows", func(t *testing.T) {
		sqltextcreate := `
CREATE PROCEDURE abassign
   @aid INT = 5,
   @bid INT = NULL OUTPUT,
   @cstr NVARCHAR(2000) = NULL OUTPUT,
   @datetime datetime = NULL OUTPUT
AS
BEGIN
   SELECT @bid = @aid, @cstr = 'OK', @datetime = '2010-01-01T00:00:00';
END;
`
		sqltextdrop := `DROP PROCEDURE abassign;`
		sqltextrun := `abassign`

		db.ExecContext(ctx, sqltextdrop)
		_, err = db.ExecContext(ctx, sqltextcreate)
		if err != nil {
			t.Fatal(err)
		}
		defer db.ExecContext(ctx, sqltextdrop)
		if err != nil {
			t.Error(err)
		}

		t.Run("should work", func(t *testing.T) {
			var bout int64
			var cout string
			_, err = db.ExecContext(ctx, sqltextrun,
				sql.Named("aid", 5),
				sql.Named("bid", sql.Out{Dest: &bout}),
				sql.Named("cstr", sql.Out{Dest: &cout}),
			)
			if err != nil {
				t.Error(err)
			}

			if bout != 5 {
				t.Errorf("expected 5, got %d", bout)
			}

			if cout != "OK" {
				t.Errorf("expected OK, got %s", cout)
			}
		})

		t.Run("should work if aid is not passed", func(t *testing.T) {
			var bout int64
			var cout string
			_, err = db.ExecContext(ctx, sqltextrun,
				sql.Named("bid", sql.Out{Dest: &bout}),
				sql.Named("cstr", sql.Out{Dest: &cout}),
			)
			if err != nil {
				t.Error(err)
			}

			if bout != 5 {
				t.Errorf("expected 5, got %d", bout)
			}

			if cout != "OK" {
				t.Errorf("expected OK, got %s", cout)
			}
		})

		t.Run("should work for DateTime1 parameter", func(t *testing.T) {
			tin, err := time.Parse(time.RFC3339, "2006-01-02T22:04:05-07:00")
			if err != nil {
				t.Fatal(err)
			}
			expected, err := time.Parse(time.RFC3339, "2010-01-01T00:00:00-00:00")
			if err != nil {
				t.Fatal(err)
			}
			datetime_param := DateTime1(tin)
			_, err = db.ExecContext(ctx, sqltextrun,
				sql.Named("datetime", sql.Out{Dest: &datetime_param}),
			)
			if err != nil {
				t.Error(err)
			}
			if time.Time(datetime_param).UTC() != expected.UTC() {
				t.Errorf("Datetime returned '%v' does not match expected value '%v'",
					time.Time(datetime_param).UTC(), expected.UTC())
			}
		})

		t.Run("destination is not a pointer", func(t *testing.T) {
			var int_out int64
			var str_out string
			// test when destination is not a pointer
			_, actual := db.ExecContext(ctx, sqltextrun,
				sql.Named("bid", sql.Out{Dest: int_out}),
				sql.Named("cstr", sql.Out{Dest: &str_out}),
			)
			pattern := ".*destination not a pointer.*"
			match, err := regexp.MatchString(pattern, actual.Error())
			if err != nil {
				t.Error(err)
			}
			if !match {
				t.Errorf("Error  '%v', does not match pattern '%v'.", actual, pattern)
			}
		})

		t.Run("should convert int64 to int", func(t *testing.T) {
			var bout int
			var cout string
			_, err := db.ExecContext(ctx, sqltextrun,
				sql.Named("bid", sql.Out{Dest: &bout}),
				sql.Named("cstr", sql.Out{Dest: &cout}),
			)
			if err != nil {
				t.Error(err)
			}

			if bout != 5 {
				t.Errorf("expected 5, got %d", bout)
			}
		})

		t.Run("should fail if destination has invalid type", func(t *testing.T) {
			// Error type should not be supported
			var err_out Error
			_, err := db.ExecContext(ctx, sqltextrun,
				sql.Named("bid", sql.Out{Dest: &err_out}),
			)
			if err == nil {
				t.Error("Expected to fail but it didn't")
			}

			// double inderection should not work
			var out_out = sql.Out{Dest: &err_out}
			_, err = db.ExecContext(ctx, sqltextrun,
				sql.Named("bid", sql.Out{Dest: out_out}),
			)
			if err == nil {
				t.Error("Expected to fail but it didn't")
			}
		})

		t.Run("should fail if parameter has invalid type", func(t *testing.T) {
			// passing invalid parameter type
			var err_val Error
			_, err = db.ExecContext(ctx, sqltextrun, err_val)
			if err == nil {
				t.Error("Expected to fail but it didn't")
			}
		})

		t.Run("destination is a nil pointer", func(t *testing.T) {
			var str_out string
			// test when destination is nil pointer
			_, actual := db.ExecContext(ctx, sqltextrun,
				sql.Named("bid", sql.Out{Dest: nil}),
				sql.Named("cstr", sql.Out{Dest: &str_out}),
			)
			pattern := ".*destination is a nil pointer.*"
			match, err := regexp.MatchString(pattern, actual.Error())
			if err != nil {
				t.Error(err)
			}
			if !match {
				t.Errorf("Error  '%v', does not match pattern '%v'.", actual, pattern)
			}
		})

		t.Run("destination is a nil pointer 2", func(t *testing.T) {
			var int_ptr *int
			_, actual := db.ExecContext(ctx, sqltextrun,
				sql.Named("bid", sql.Out{Dest: int_ptr}),
			)
			pattern := ".*destination is a nil pointer.*"
			match, err := regexp.MatchString(pattern, actual.Error())
			if err != nil {
				t.Error(err)
			}
			if !match {
				t.Errorf("Error  '%v', does not match pattern '%v'.", actual, pattern)
			}
		})

		t.Run("pointer to a pointer", func(t *testing.T) {
			var str_out *string
			_, actual := db.ExecContext(ctx, sqltextrun,
				sql.Named("cstr", sql.Out{Dest: &str_out}),
			)
			pattern := ".*destination is a pointer to a pointer.*"
			match, err := regexp.MatchString(pattern, actual.Error())
			if err != nil {
				t.Error(err)
			}
			if !match {
				t.Errorf("Error  '%v', does not match pattern '%v'.", actual, pattern)
			}
		})

		t.Run("query with rows", func(t *testing.T) {
			sqltext := `
SELECT @param1 = 'Hello'
;
SELECT 'Hi'
;
SELECT @param2 = 'World'
`
			var param1, param2 string
			rows, err := db.QueryContext(ctx, sqltext, sql.Named("param1", sql.Out{Dest: &param1}), sql.Named("param2", sql.Out{Dest: &param2}))
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()

			if !rows.Next() {
				t.Error("Next returned false")
			}
			var rowval string
			err = rows.Scan(&rowval)
			if err != nil {
				t.Error(err)
			}
			if rowval != "Hi" {
				t.Errorf(`expected "Hi", got %#v`, rowval)
			}
			if rows.Next() {
				t.Error("Next returned true but should return false after last row was returned")
			}

			// Output parameters should be filled when the resultset has been thoroughly read
			if param1 != "Hello" {
				t.Errorf(`@param1: expected "Hello", got %#v`, param1)
			}

			if param2 != "World" {
				t.Errorf(`@param2: expected "World", got %#v`, param2)
			}
		})
	})
}

func TestOutputINOUTParam(t *testing.T) {
	sqltextcreate := `
CREATE PROCEDURE abinout
   @aid INT = 1,
   @bid INT = 2 OUTPUT,
   @cstr NVARCHAR(2000) = NULL OUTPUT,
   @vout VARCHAR(2000) = NULL OUTPUT,
   @nullint INT = NULL OUTPUT,
   @nullfloat FLOAT = NULL OUTPUT,
   @nullstr NVARCHAR(10) = NULL OUTPUT,
   @nullbit BIT = NULL OUTPUT,
   @varbin VARBINARY(10) = NULL OUTPUT
AS
BEGIN
   SELECT
		@bid = @aid + @bid,
		@cstr = 'OK',
		@vout = 'DREAM'
	;
END;
`
	sqltextdrop := `DROP PROCEDURE abinout;`
	sqltextrun := `abinout`

	checkConnStr(t)
	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)

	db, err := sql.Open("sqlserver", makeConnStr(t).String())
	if err != nil {
		t.Fatalf("failed to open driver sqlserver")
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db.ExecContext(ctx, sqltextdrop)
	_, err = db.ExecContext(ctx, sqltextcreate)
	if err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, sqltextdrop)

	t.Run("original test", func(t *testing.T) {
		var bout int64 = 3
		var cout string
		var vout VarChar
		_, err = db.ExecContext(ctx, sqltextrun,
			sql.Named("aid", 5),
			sql.Named("bid", sql.Out{Dest: &bout}),
			sql.Named("cstr", sql.Out{Dest: &cout}),
			sql.Named("vout", sql.Out{Dest: &vout}),
		)
		if err != nil {
			t.Error(err)
		}

		if bout != 8 {
			t.Errorf("expected 8, got %d", bout)
		}

		if cout != "OK" {
			t.Errorf("expected OK, got %s", cout)
		}
		if string(vout) != "DREAM" {
			t.Errorf("expected DREAM, got %s", vout)
		}
	})

	t.Run("test null values returned into nullable", func(t *testing.T) {
		var nullint sql.NullInt64
		var nullfloat sql.NullFloat64
		var nullstr sql.NullString
		var nullbit sql.NullBool
		_, err = db.ExecContext(ctx, sqltextrun,
			sql.Named("nullint", sql.Out{Dest: &nullint}),
			sql.Named("nullfloat", sql.Out{Dest: &nullfloat}),
			sql.Named("nullstr", sql.Out{Dest: &nullstr}),
			sql.Named("nullbit", sql.Out{Dest: &nullbit}),
		)
		if err != nil {
			t.Error(err)
		}

		if nullint.Valid {
			t.Errorf("expected NULL, got %v", nullint)
		}
		if nullfloat.Valid {
			t.Errorf("expected NULL, got %v", nullfloat)
		}
		if nullstr.Valid {
			t.Errorf("expected NULL, got %v", nullstr)
		}
		if nullbit.Valid {
			t.Errorf("expected NULL, got %v", nullbit)
		}
	})

	// Not yet supported
	//t.Run("test null values returned into pointers", func(t *testing.T) {
	//	var nullint *int64
	//	var nullfloat *float64
	//	var nullstr *string
	//	var nullbit *bool
	//	_, err = db.ExecContext(ctx, sqltextrun,
	//		sql.Named("nullint", sql.Out{Dest: &nullint}),
	//		sql.Named("nullfloat", sql.Out{Dest: &nullfloat}),
	//		sql.Named("nullstr", sql.Out{Dest: &nullstr}),
	//		sql.Named("nullbit", sql.Out{Dest: &nullbit}),
	//	)
	//	if err != nil {
	//		t.Error(err)
	//	}

	//	if nullint != nil {
	//		t.Errorf("expected NULL, got %v", nullint)
	//	}
	//	if nullfloat != nil {
	//		t.Errorf("expected NULL, got %v", nullfloat)
	//	}
	//	if nullstr != nil {
	//		t.Errorf("expected NULL, got %v", nullstr)
	//	}
	//	if nullbit != nil {
	//		t.Errorf("expected NULL, got %v", nullbit)
	//	}
	//})

	t.Run("test non null values into nullable", func(t *testing.T) {
		nullint := sql.NullInt64{Int64: 10, Valid: true}
		nullfloat := sql.NullFloat64{Float64: 1.5, Valid: true}
		nullstr := sql.NullString{String: "hello", Valid: true}
		nullbit := sql.NullBool{Bool: true, Valid: true}
		_, err = db.ExecContext(ctx, sqltextrun,
			sql.Named("nullint", sql.Out{Dest: &nullint}),
			sql.Named("nullfloat", sql.Out{Dest: &nullfloat}),
			sql.Named("nullstr", sql.Out{Dest: &nullstr}),
			sql.Named("nullbit", sql.Out{Dest: &nullbit}),
		)
		if err != nil {
			t.Error(err)
		}
		if !nullint.Valid {
			t.Error("expected non null value, but got null")
		}
		if nullint.Int64 != 10 {
			t.Errorf("expected 10, got %d", nullint.Int64)
		}
		if !nullfloat.Valid {
			t.Error("expected non null value, but got null")
		}
		if nullfloat.Float64 != 1.5 {
			t.Errorf("expected 1.5, got %v", nullfloat.Float64)
		}
		if !nullstr.Valid {
			t.Error("expected non null value, but got null")
		}
		if nullstr.String != "hello" {
			t.Errorf("expected hello, got %s", nullstr.String)
		}
	})
	t.Run("test return into byte[]", func(t *testing.T) {
		cstr := []byte{1, 2, 3}
		_, err = db.ExecContext(ctx, sqltextrun,
			sql.Named("varbin", sql.Out{Dest: &cstr}),
		)
		if err != nil {
			t.Error(err)
		}
		expected := []byte{1, 2, 3}
		if !bytes.Equal(cstr, expected) {
			t.Errorf("expected [1,2,3], got %v", cstr)
		}
	})
	t.Run("test int into string", func(t *testing.T) {
		var str string
		_, err = db.ExecContext(ctx, sqltextrun,
			sql.Named("bid", sql.Out{Dest: &str}),
		)
		if err != nil {
			t.Error(err)
		}
		if str != "1" {
			t.Errorf("expected '1', got %v", str)
		}
	})
	t.Run("typeless null for output parameter should return error", func(t *testing.T) {
		var val interface{}
		_, actual := db.ExecContext(ctx, sqltextrun,
			sql.Named("bid", sql.Out{Dest: &val}),
		)
		if actual == nil {
			t.Error("Expected to fail but didn't")
		}
		pattern := ".*MSSQL does not allow NULL value without type for OUTPUT parameters.*"
		match, err := regexp.MatchString(pattern, actual.Error())
		if err != nil {
			t.Error(err)
		}
		if !match {
			t.Errorf("Error  '%v', does not match pattern '%v'.", actual, pattern)
		}
	})
}

// TestOutputParamWithRows tests reading output parameter after retrieving rows from the result set
// of a stored procedure. SQL Server sends output parameters after all the rows are returned.
// Therefore, if the output parameter is read before all the rows are retrieved, the value will be
// incorrect. Furthermore, the Data Race Detector would detect a data race because the output
// variable is shared between the driver and the client application.
//
// Issue https://github.com/denisenkom/go-mssqldb/issues/378
func TestOutputParamWithRows(t *testing.T) {
	sqltextcreate := `
	CREATE PROCEDURE spwithoutputandrows
		@bitparam BIT OUTPUT
	AS BEGIN
		SET @bitparam = 1
		SELECT 'Row 1'
	END
	`
	sqltextdrop := `DROP PROCEDURE spwithoutputandrows;`
	sqltextrun := `spwithoutputandrows`

	checkConnStr(t)
	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)

	db, err := sql.Open("sqlserver", makeConnStr(t).String())
	if err != nil {
		t.Fatalf("failed to open driver sqlserver")
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db.ExecContext(ctx, sqltextdrop)
	_, err = db.ExecContext(ctx, sqltextcreate)
	if err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, sqltextdrop)

	t.Run("Retrieve output after reading rows", func(t *testing.T) {
		var bitout int64 = 5
		rows, err := db.QueryContext(ctx, sqltextrun, sql.Named("bitparam", sql.Out{Dest: &bitout}))
		if err != nil {
			t.Error(err)
		} else {
			defer rows.Close()
			// If the output parameter is read all the rows are retrieved:
			// 1. The output parameter remains that same (int this case, bitout = 5)
			// 2. Data Race Detector reports a Data Race because bitout is being shared by the driver and the client application
			/*
				if bitout != 5 {
					t.Errorf("expected bitout to remain as 5, got %d", bitout)
				}
			*/
			var strrow string
			for rows.Next() {
				err = rows.Scan(&strrow)
				if err != nil {
					t.Fatal("scan failed", err)
				}
			}
			if bitout != 1 {
				t.Errorf("expected 1, got %d", bitout)
			}
		}
	})
}

func TestParamNoName(t *testing.T) {
	checkConnStr(t)
	tl := testLogger{t: t}
	defer tl.StopLogging()
	SetLogger(&tl)

	db, err := sql.Open("sqlserver", makeConnStr(t).String())
	if err != nil {
		t.Fatalf("failed to open driver sqlserver")
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checkResults := func(r *sql.Rows, tInner *testing.T) {
		var intCol int
		var nvarcharCol string
		var varcharCol string
		for r.Next() {
			err = r.Scan(&intCol, &nvarcharCol, &varcharCol)
		}
		if intCol != 5 {
			tInner.Errorf("expected 5, got %d", intCol)
		}
		if nvarcharCol != "OK" {
			tInner.Errorf("expected OK, got %s", nvarcharCol)
		}
		if varcharCol != "DREAM" {
			tInner.Errorf("expected DREAM, got %s", varcharCol)
		}
	}

	t.Run("Execute stored prodecure", func(t *testing.T) {
		sqltextcreate := `
		CREATE PROCEDURE spnoparamname
			@intCol INT,
			@nvarcharCol NVARCHAR(2000),
			@varcharCol VARCHAR(2000)
		AS BEGIN
			SELECT @intCol, @nvarcharCol, @varcharCol
		END`
		sqltextdrop := `DROP PROCEDURE spnoparamname`
		sqltextrun := `spnoparamname`

		db.ExecContext(ctx, sqltextdrop)
		_, err = db.ExecContext(ctx, sqltextcreate)
		if err != nil {
			t.Fatal(err)
		}
		defer db.ExecContext(ctx, sqltextdrop)

		t.Run("With no parameter names", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, sqltextrun, 5, "OK", "DREAM")
			if err != nil {
				t.Error(err)
			} else {
				defer rows.Close()
				checkResults(rows, t)
			}
		})

		t.Run("With parameter names", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, sqltextrun, sql.Named("intCol", 5), sql.Named("nvarcharCol", "OK"), sql.Named("varcharCol", "DREAM"))
			if err != nil {
				t.Error(err)
			} else {
				defer rows.Close()
				checkResults(rows, t)
			}
		})
	})

	t.Run("Execute query", func(t *testing.T) {
		sqltextrun := "SELECT @p1, @p2, @p3"

		t.Run("With no parameter names", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, sqltextrun, 5, "OK", "DREAM")
			if err != nil {
				t.Error(err)
			} else {
				defer rows.Close()
				checkResults(rows, t)
			}
		})

		t.Run("With parameter names", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, sqltextrun, sql.Named("p1", 5), sql.Named("p2", "OK"), sql.Named("p3", "DREAM"))
			if err != nil {
				t.Error(err)
			} else {
				defer rows.Close()
				checkResults(rows, t)
			}
		})
	})
}

// TestTLSServerReadClose tests writing to an encrypted database connection.
// Currently the database server will close the connection while the server is
// reading the TDS packets and before any of the data has been parsed.
//
// When two queries are sent in reverse order, they PASS, but if we send only
// a single ping (SELECT 1;) first, then the long query the query fails.
//
// The long query text is never parsed. In fact, you can comment out, return
// early, or have malformed sql in the long query text. Just the length matters.
// The error happens when sending the TDS Batch packet to SQL Server the server
// closes the connection..
//
// It appears the driver sends valid TDS packets. In fact, if prefixed with 4
// "SELECT 1;" TDS Batch queries then the long query works, but if zero or one
// "SELECT 1;" TDS Batch queries are send prior the long query fails to send.
//
// Lastly, this only manafests itself with an encrypted connection. This has been
// observed with SQL Server Azure, SQL Server 13.0.1742 on Windows, and SQL Server
// 14.0.900.75 on Linux. It also fails when using the "dev.boringcrypto" (a C based
// TLS crypto). I haven't found any knobs on SQL Server to expose the error message
// nor have I found a good way to decrypt the TDS stream. KeyLogWriter in the TLS
// config may help with that, but wireshark wasn't decrypting TDS based TLS streams
// even when using that.
//
// Issue https://github.com/denisenkom/go-mssqldb/issues/166
func TestTLSServerReadClose(t *testing.T) {
	query := `
with
    config_cte (config) as (
            select *
                    from ( values
                    ('_partition:{\"Fill\":{\"PatternType\":\"solid\",\"FgColor\":\"99ff99\"}}')
                    , ('_separation:{\"Fill\":{\"PatternType\":\"solid\",\"FgColor\":\"99ffff\"}}')
                    , ('Monthly Earnings:\$#,##0.00 ;(\$#,##0.00)')
                    , ('Weekly Earnings:\$#,##0.00 ;(\$#,##0.00)')
                    , ('Total Earnings:\$#,##0.00 ;(\$#,##0.00)')
                    , ('Average Earnings:\$#,##0.00 ;(\$#,##0.00)')
                    , ('Last Month Earning:#,##0.00 ;(#,##0.00)')
                    , ('Award:\$#,##0.00 ;(\$#,##0.00)')
                    , ('Amount:\$#,##0.00 ;(\$#,##0.00)')
                    , ('Grand Total:\$#,##0.00 ;(\$#,##0.00)')
                    , ('Total:\$#,##0.00 ;(\$#,##0.00)')
                    , ('Price Each:\$#,##0.00 ;(\$#,##0.00)')
                    , ('Hyperwallet:\$#,##0.00 ;(\$#,##0.00)')
                    , ('Credit/Debit:\$#,##0.00 ;(\$#,##0.00)')
                    , ('Earning:#,##0.00 ;(#,##0.00)')
                    , ('Change Earning:#,##0.00 ;(#,##0.00)')
                    , ('CheckAmount:#,##0.00 ;(#,##0.00)')
                    , ('Residual:#,##0.00 ;(#,##0.00)')
                    , ('Prev Residual:#,##0.00 ;(#,##0.00)')
                    , ('Team Bonuses:#,##0.00 ;(#,##0.00)')
                    , ('Change:#,##0.00 ;(#,##0.00)')
                    , ('Shipping Total:#,##0.00 ;(#,##0.00)')
                    , ('SubTotal:\$#,##0.00 ;(\$#,##0.00)')
                    , ('Total Diff:#,##0.00 ;(#,##0.00)')
                    , ('SubTotal Diff:#,##0.00 ;(#,##0.00)')
                    , ('Return Total:#,##0.00 ;(#,##0.00)')
                    , ('Return SubTotal:#,##0.00 ;(#,##0.00)')
                    , ('Return Total Diff:#,##0.00 ;(#,##0.00)')
                    , ('Return SubTotal Diff:#,##0.00 ;(#,##0.00)')
                    , ('Cancel Total:#,##0.00 ;(#,##0.00)')
                    , ('Cancel SubTotal:#,##0.00 ;(#,##0.00)')
                    , ('Cancel Total Diff:#,##0.00 ;(#,##0.00)')
                    , ('Cancel SubTotal Diff:#,##0.00 ;(#,##0.00)')
                    , ('Replacement Total:#,##0.00 ;(#,##0.00)')
                    , ('Replacement SubTotal:#,##0.00 ;(#,##0.00)')
                    , ('Replacement Total Diff:#,##0.00 ;(#,##0.00)')
                    , ('Replacement SubTotal Diff:#,##0.00 ;(#,##0.00)')
                    , ('Jan Residual:#,##0.00 ;(#,##0.00)')
                    , ('Jan Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Jan Total:#,##0.00 ;(#,##0.00)')
                    , ('January Residual:#,##0.00 ;(#,##0.00)')
                    , ('Feb Residual:#,##0.00 ;(#,##0.00)')
                    , ('Feb Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Feb Total:#,##0.00 ;(#,##0.00)')
                    , ('February Residual:#,##0.00 ;(#,##0.00)')
                    , ('Mar Residual:#,##0.00 ;(#,##0.00)')
                    , ('Mar Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Mar Total:#,##0.00 ;(#,##0.00)')
                    , ('March Residual:#,##0.00 ;(#,##0.00)')
                    , ('Apr Residual:#,##0.00 ;(#,##0.00)')
                    , ('Apr Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Apr Total:#,##0.00 ;(#,##0.00)')
                    , ('April Residual:#,##0.00 ;(#,##0.00)')
                    , ('May Residual:#,##0.00 ;(#,##0.00)')
                    , ('May Bonus:#,##0.00 ;(#,##0.00)')
                    , ('May Total:#,##0.00 ;(#,##0.00)')
                    , ('Jun Residual:#,##0.00 ;(#,##0.00)')
                    , ('Jun Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Jun Total:#,##0.00 ;(#,##0.00)')
                    , ('June Residual:#,##0.00 ;(#,##0.00)')
                    , ('Jul Residual:#,##0.00 ;(#,##0.00)')
                    , ('Jul Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Jul Total:#,##0.00 ;(#,##0.00)')
                    , ('July Residual:#,##0.00 ;(#,##0.00)')
                    , ('Aug Residual:#,##0.00 ;(#,##0.00)')
                    , ('Aug Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Aug Total:#,##0.00 ;(#,##0.00)')
                    , ('August Residual:#,##0.00 ;(#,##0.00)')
                    , ('Sep Residual:#,##0.00 ;(#,##0.00)')
                    , ('Sep Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Sep Total:#,##0.00 ;(#,##0.00)')
                    , ('September Residual:#,##0.00 ;(#,##0.00)')
                    , ('Oct Residual:#,##0.00 ;(#,##0.00)')
                    , ('Oct Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Oct Total:#,##0.00 ;(#,##0.00)')
                    , ('October Residual:#,##0.00 ;(#,##0.00)')
                    , ('Nov Residual:#,##0.00 ;(#,##0.00)')
                    , ('Nov Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Nov Total:#,##0.00 ;(#,##0.00)')
                    , ('November Residual:#,##0.00 ;(#,##0.00)')
                    , ('Dec Residual:#,##0.00 ;(#,##0.00)')
                    , ('Dec Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Dec Total:#,##0.00 ;(#,##0.00)')
                    , ('December Residual:#,##0.00 ;(#,##0.00)')
                    , ('January Bonus:#,##0.00 ;(#,##0.00)')
                    , ('February Bonus:#,##0.00 ;(#,##0.00)')
                    , ('March Bonus:#,##0.00 ;(#,##0.00)')
                    , ('April Bonus:#,##0.00 ;(#,##0.00)')
                    , ('May Bonus:#,##0.00 ;(#,##0.00)')
                    , ('June Bonus:#,##0.00 ;(#,##0.00)')
                    , ('July Bonus:#,##0.00 ;(#,##0.00)')
                    , ('August Bonus:#,##0.00 ;(#,##0.00)')
                    , ('September Bonus:#,##0.00 ;(#,##0.00)')
                    , ('October Bonus:#,##0.00 ;(#,##0.00)')
                    , ('November Bonus:#,##0.00 ;(#,##0.00)')
                    , ('December Bonus:#,##0.00 ;(#,##0.00)')
                    , ('January Adj:#,##0.00 ;(#,##0.00)')
                    , ('February Adj:#,##0.00 ;(#,##0.00)')
                    , ('March Adj:#,##0.00 ;(#,##0.00)')
                    , ('April Adj:#,##0.00 ;(#,##0.00)')
                    , ('May Adj:#,##0.00 ;(#,##0.00)')
                    , ('June Adj:#,##0.00 ;(#,##0.00)')
                    , ('July Adj:#,##0.00 ;(#,##0.00)')
                    , ('August Adj:#,##0.00 ;(#,##0.00)')
                    , ('September Adj:#,##0.00 ;(#,##0.00)')
                    , ('October Adj:#,##0.00 ;(#,##0.00)')
                    , ('November Adj:#,##0.00 ;(#,##0.00)')
                    , ('December Adj:#,##0.00 ;(#,##0.00)')
                    , ('2016- 2015 YTD Dif:#,##0.00 ;(#,##0.00)')
                    , ('2017- 2016 YTD Dif:#,##0.00 ;(#,##0.00)')
                    , ('2018- 2017 YTD Dif:#,##0.00 ;(#,##0.00)')
                    , ('Dec to Jan Dif Residual:#,##0.00 ;(#,##0.00)')
                    , ('Jan to Feb Dif Residual:#,##0.00 ;(#,##0.00)')
                    , ('Feb to Mar Dif Residual:#,##0.00 ;(#,##0.00)')
                    , ('Mar to Apr Dif Residual:#,##0.00 ;(#,##0.00)')
                    , ('Apr to May Dif Residual:#,##0.00 ;(#,##0.00)')
                    , ('May to Jun Dif Residual:#,##0.00 ;(#,##0.00)')
                    , ('Jun to Jul Dif Residual:#,##0.00 ;(#,##0.00)')
                    , ('Jul to Aug Dif Residual:#,##0.00 ;(#,##0.00)')
                    , ('Aug to Sep Dif Residual:#,##0.00 ;(#,##0.00)')
                    , ('Sep to Oct Dif Residual:#,##0.00 ;(#,##0.00)')
                    , ('Oct to Nov Dif Residual:#,##0.00 ;(#,##0.00)')
                    , ('Nov to Dec Dif Residual:#,##0.00 ;(#,##0.00)')
                    , ('Dec to Jan Dif Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Jan to Feb Dif Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Feb to Mar Dif Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Mar to Apr Dif Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Apr to May Dif Bonus:#,##0.00 ;(#,##0.00)')
                    , ('May to Jun Dif Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Jun to Jul Dif Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Jul to Aug Dif Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Aug to Sep Dif Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Sep to Oct Dif Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Oct to Nov Dif Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Nov to Dec Dif Bonus:#,##0.00 ;(#,##0.00)')
                    , ('Dec to Jan Dif Total:#,##0.00 ;(#,##0.00)')
                    , ('Jan to Feb Dif Total:#,##0.00 ;(#,##0.00)')
                    , ('Feb to Mar Dif Total:#,##0.00 ;(#,##0.00)')
                    , ('Mar to Apr Dif Total:#,##0.00 ;(#,##0.00)')
                    , ('Apr to May Dif Total:#,##0.00 ;(#,##0.00)')
                    , ('May to Jun Dif Total:#,##0.00 ;(#,##0.00)')
                    , ('Jun to Jul Dif Total:#,##0.00 ;(#,##0.00)')
                    , ('Jul to Aug Dif Total:#,##0.00 ;(#,##0.00)')
                    , ('Aug to Sep Dif Total:#,##0.00 ;(#,##0.00)')
                    , ('Sep to Oct Dif Total:#,##0.00 ;(#,##0.00)')
                    , ('Oct to Nov Dif Total:#,##0.00 ;(#,##0.00)')
                    , ('Nov to Dec Dif Total:#,##0.00 ;(#,##0.00)')
                    , ('Jan Refund Cnt:#,##0 ;(#,##0)')
                    , ('Feb Refund Cnt:#,##0 ;(#,##0)')
                    , ('Mar Refund Cnt:#,##0 ;(#,##0)')
                    , ('Apr Refund Cnt:#,##0 ;(#,##0)')
                    , ('May Refund Cnt:#,##0 ;(#,##0)')
                    , ('Jun Refund Cnt:#,##0 ;(#,##0)')
                    , ('Jul Refund Cnt:#,##0 ;(#,##0)')
                    , ('Aug Refund Cnt:#,##0 ;(#,##0)')
                    , ('Sep Refund Cnt:#,##0 ;(#,##0)')
                    , ('Oct Refund Cnt:#,##0 ;(#,##0)')
                    , ('Nov Refund Cnt:#,##0 ;(#,##0)')
                    , ('Dec Refund Cnt:#,##0 ;(#,##0)')
                    , ('Jan Purchase Cnt:#,##0 ;(#,##0)')
                    , ('Feb Purchase Cnt:#,##0 ;(#,##0)')
                    , ('Mar Purchase Cnt:#,##0 ;(#,##0)')
                    , ('Apr Purchase Cnt:#,##0 ;(#,##0)')
                    , ('May Purchase Cnt:#,##0 ;(#,##0)')
                    , ('Jun Purchase Cnt:#,##0 ;(#,##0)')
                    , ('Jul Purchase Cnt:#,##0 ;(#,##0)')
                    , ('Aug Purchase Cnt:#,##0 ;(#,##0)')
                    , ('Sep Purchase Cnt:#,##0 ;(#,##0)')
                    , ('Oct Purchase Cnt:#,##0 ;(#,##0)')
                    , ('Nov Purchase Cnt:#,##0 ;(#,##0)')
                    , ('Dec Purchase Cnt:#,##0 ;(#,##0)')
                    , ('Jan Refund Amt:#,##0.00 ;(#,##0.00)')
                    , ('Feb Refund Amt:#,##0.00 ;(#,##0.00)')
                    , ('Mar Refund Amt:#,##0.00 ;(#,##0.00)')
                    , ('Apr Refund Amt:#,##0.00 ;(#,##0.00)')
                    , ('May Refund Amt:#,##0.00 ;(#,##0.00)')
                    , ('Jun Refund Amt:#,##0.00 ;(#,##0.00)')
                    , ('Jul Refund Amt:#,##0.00 ;(#,##0.00)')
                    , ('Aug Refund Amt:#,##0.00 ;(#,##0.00)')
                    , ('Sep Refund Amt:#,##0.00 ;(#,##0.00)')
                    , ('Oct Refund Amt:#,##0.00 ;(#,##0.00)')
                    , ('Nov Refund Amt:#,##0.00 ;(#,##0.00)')
                    , ('Dec Refund Amt:#,##0.00 ;(#,##0.00)')
                    , ('Jan Purchase Amt:#,##0.00 ;(#,##0.00)')
                    , ('Feb Purchase Amt:#,##0.00 ;(#,##0.00)')
                    , ('Mar Purchase Amt:#,##0.00 ;(#,##0.00)')
                    , ('Apr Purchase Amt:#,##0.00 ;(#,##0.00)')
                    , ('May Purchase Amt:#,##0.00 ;(#,##0.00)')
                    , ('Jun Purchase Amt:#,##0.00 ;(#,##0.00)')
                    , ('Jul Purchase Amt:#,##0.00 ;(#,##0.00)')
                    , ('Aug Purchase Amt:#,##0.00 ;(#,##0.00)')
                    , ('Sep Purchase Amt:#,##0.00 ;(#,##0.00)')
                    , ('Oct Purchase Amt:#,##0.00 ;(#,##0.00)')
                    , ('Nov Purchase Amt:#,##0.00 ;(#,##0.00)')
                    , ('Dec Purchase Amt:#,##0.00 ;(#,##0.00)')
                    ) X(a))
    select * from config_cte
	`
	t.Logf("query len (utf16 bytes)=%d, len/4096=%f\n", len(query)*2, float64(len(query)*2)/4096)

	db, logger := open(t)
	defer db.Close()
	defer logger.StopLogging()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type run struct {
		name  string
		pings []int
		pass  bool

		conn *sql.Conn
	}

	// Use separate Conns from the connection pool to ensure separation.
	runs := []*run{
		{name: "rev", pings: []int{4, 1}, pass: true},
		{name: "forward", pings: []int{1}, pass: true},
	}
	for _, r := range runs {
		var err error
		r.conn, err = db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer r.conn.Close()
	}

	for _, r := range runs {
		for _, ping := range r.pings {
			t.Run(fmt.Sprintf("%s-ping-%d", r.name, ping), func(t *testing.T) {
				for i := 0; i < ping; i++ {
					if err := r.conn.PingContext(ctx); err != nil {
						if r.pass {
							t.Error("failed to ping server", err)
						} else {
							t.Log("failed to ping server", err)
						}
						return
					}
				}

				rows, err := r.conn.QueryContext(ctx, query)
				if err != nil {
					if r.pass {
						t.Errorf("QueryContext: %+v", err)
					} else {
						t.Logf("QueryContext: %+v", err)
					}
					return
				}
				defer rows.Close()
				for rows.Next() {
					// Nothing.
				}
			})
		}
	}
}

func TestDateTimeParam19(t *testing.T) {
	conn, logger := open(t)
	defer conn.Close()
	logger.StopLogging()

	// testing DateTime1, only supported on go 1.9
	var emptydate time.Time
	mindate1 := time.Date(1753, 1, 1, 0, 0, 0, 0, time.UTC)
	maxdate1 := time.Date(9999, 12, 31, 23, 59, 59, 997000000, time.UTC)
	testdates1 := []DateTime1{
		DateTime1(mindate1),
		DateTime1(maxdate1),
		DateTime1(time.Date(1752, 12, 31, 23, 59, 59, 997000000, time.UTC)), // just a little below minimum date
		DateTime1(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)),             // just a little over maximum date
		DateTime1(emptydate),
	}

	for _, test := range testdates1 {
		t.Run(fmt.Sprintf("Test datetime for %v", test), func(t *testing.T) {
			var res time.Time
			expected := time.Time(test)
			queryParamRoundTrip(conn, test, &res)
			// clip value
			if expected.Before(mindate1) {
				expected = mindate1
			}
			if expected.After(maxdate1) {
				expected = maxdate1
			}
			if expected.Sub(res) != 0 {
				t.Errorf("expected: '%s', got: '%s' delta: %d", expected, res, expected.Sub(res))
			}
		})
	}
}

func TestReturnStatus(t *testing.T) {
	conn, logger := open(t)
	defer conn.Close()
	defer logger.StopLogging()

	_, err := conn.Exec("if object_id('retstatus') is not null drop proc retstatus;")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec("create proc retstatus as return 2;")
	if err != nil {
		t.Fatal(err)
	}

	var rs ReturnStatus
	_, err = conn.Exec("retstatus", &rs)
	conn.Exec("drop proc retstatus;")
	if err != nil {
		t.Fatal(err)
	}
	if rs != 2 {
		t.Errorf("expected status=2, got %d", rs)
	}
}

func TestClearReturnStatus(t *testing.T) {
	db, logger := open(t)
	defer db.Close()
	defer logger.StopLogging()

	ctx := context.TODO()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.ExecContext(ctx, "CREATE PROC #get_answer AS RETURN 42")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.ExecContext(ctx, "CREATE PROC #get_half AS RETURN 21")
	if err != nil {
		t.Fatal(err)
	}

	var rs ReturnStatus

	_, err = conn.ExecContext(ctx, "#get_answer", &rs)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.ExecContext(ctx, "#get_half")
	if err != nil {
		t.Fatal(err)
	}

	if rs != 42 {
		t.Errorf("expected status=42, got %d", rs)
	}
}

func TestMessageQueue(t *testing.T) {
	conn, logger := open(t)
	defer conn.Close()
	defer logger.StopLogging()
	retmsg := &sqlexp.ReturnMessage{}
	latency, _ := getLatency(t)
	ctx, cancel := context.WithTimeout(context.Background(), latency+200000*time.Millisecond)
	defer cancel()
	rows, err := conn.QueryContext(ctx, "PRINT 'msg1'; select 100 as c; PRINT 'msg2'", retmsg)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer rows.Close()
	active := true

	msgs := []interface{}{
		sqlexp.MsgNotice{Message: "msg1"},
		sqlexp.MsgNext{},
		sqlexp.MsgRowsAffected{Count: 1},
		sqlexp.MsgNotice{Message: "msg2"},
		sqlexp.MsgNextResultSet{},
	}
	i := 0
	rsCount := 0
	for active {
		msg := retmsg.Message(ctx)
		if i >= len(msgs) {
			t.Fatalf("Got extra message:%+v", msg)
		}
		t.Log(reflect.TypeOf(msg))
		if reflect.TypeOf(msgs[i]) != reflect.TypeOf(msg) {
			t.Fatalf("Out of order or incorrect message at %d. Actual: %+v. Expected: %+v", i, reflect.TypeOf(msg), reflect.TypeOf(msgs[i]))
		}
		switch m := msg.(type) {
		case sqlexp.MsgNotice:
			t.Log(m.Message)
		case sqlexp.MsgNextResultSet:
			active = rows.NextResultSet()
			if active {
				t.Fatal("NextResultSet returned true")
			}
			rsCount++
		case sqlexp.MsgNext:
			if !rows.Next() {
				t.Fatal("rows.Next() returned false")
			}
			var c int
			err = rows.Scan(&c)
			if err != nil {
				t.Fatalf("rows.Scan() failed: %s", err.Error())
			}
			if c != 100 {
				t.Fatalf("query returned wrong value: %d", c)
			}
		}
		i++
	}
}

func TestAdvanceResultSetAfterPartialRead(t *testing.T) {
	conn, logger := open(t)
	defer conn.Close()
	defer logger.StopLogging()

	ctx := context.Background()
	retmsg := &sqlexp.ReturnMessage{}

	rows, err := conn.QueryContext(ctx, "select top 2 object_id from sys.all_objects; print 'this is a message'; select 100 as Count; ", retmsg)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer rows.Close()

	rows.Next()
	var g interface{}
	err = rows.Scan(&g)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	next := rows.NextResultSet()
	if !next {
		t.Fatalf("NextResultSet returned false")
	}
	next = rows.Next()
	if !next {
		t.Fatalf("Next on the second result set returned false")
	}
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Columns() error: %s", err)
	}
	if cols[0] != "Count" {
		t.Fatalf("Wrong column in second result:%s, expected Count", cols[0])
	}
	var c int
	err = rows.Scan(&c)
	if err != nil {
		t.Fatalf("Scan errored out on second result: %s", err)
	}
	if c != 100 {
		t.Fatalf("Scan returned incorrect value on second result set: %d, expected 100", c)
	}
}
func TestMessageQueueWithErrors(t *testing.T) {
	conn, logger := open(t)
	defer conn.Close()
	defer logger.StopLogging()

	msgs, errs, results, rowcounts := testMixedQuery(conn, t)
	if msgs != 1 {
		t.Fatalf("Got %d messages, expected 1", msgs)
	}
	if errs != 1 {
		t.Fatalf("Got %d errors, expected 1", errs)
	}
	if results != 4 {
		t.Fatalf("Got %d results, expected 4", results)
	}
	if rowcounts != 4 {
		t.Fatalf("Got %d row counts, expected 4", rowcounts)
	}
}

const mixedQuery = `select top 5 name from sys.system_columns
select getdate()
PRINT N'This is a message'
select 199
RAISERROR (N'Testing!' , 11, 1)
select 300
`

func testMixedQuery(conn *sql.DB, b testing.TB) (msgs, errs, results, rowcounts int) {
	ctx := context.Background()
	retmsg := &sqlexp.ReturnMessage{}
	r, err := conn.QueryContext(ctx, mixedQuery, retmsg)
	if err != nil {
		b.Fatal(err.Error())
	}
	defer r.Close()
	active := true
	first := true
	for active {
		msg := retmsg.Message(ctx)
		switch m := msg.(type) {
		case sqlexp.MsgNotice:
			b.Logf("MsgNotice:%s", m.Message)
			msgs++
		case sqlexp.MsgNext:
			b.Logf("MsgNext")
			inresult := true
			for inresult {
				inresult = r.Next()
				if first {
					if !inresult {
						b.Fatalf("First Next call returned false")
					}
					results++
				}
				if inresult {
					var d interface{}
					err = r.Scan(&d)
					if err != nil {
						b.Fatalf("Scan failed:%v", err)
					}
					b.Logf("Row data:%v", d)
				}
				first = false
			}
		case sqlexp.MsgNextResultSet:
			b.Log("MsgNextResultSet")
			active = r.NextResultSet()
			first = true
		case sqlexp.MsgError:
			b.Logf("MsgError:%v", m.Error)
			errs++
		case sqlexp.MsgRowsAffected:
			b.Logf("MsgRowsAffected:%d", m.Count)
			rowcounts++
		}
	}
	return msgs, errs, results, rowcounts
}

func TestTimeoutWithNoResults(t *testing.T) {
	conn, logger := open(t)
	defer conn.Close()
	defer logger.StopLogging()

	latency, _ := getLatency(t)
	ctx, cancel := context.WithTimeout(context.Background(), latency+5000*time.Millisecond)
	defer cancel()
	retmsg := &sqlexp.ReturnMessage{}
	r, err := conn.QueryContext(ctx, `waitfor delay '00:00:15'; select 100`, retmsg)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer r.Close()
	active := true
	for active {
		msg := retmsg.Message(ctx)
		t.Logf("Got a message: %s", reflect.TypeOf(msg))
		switch m := msg.(type) {
		case sqlexp.MsgNextResultSet:
			active = r.NextResultSet()
			if active {
				t.Fatal("NextResultSet returned true")
			}
		case sqlexp.MsgNext:
			if r.Next() {
				t.Fatal("Got a successful Next even though the query should have timed out")
			}
		case sqlexp.MsgRowsAffected:
			t.Fatalf("Got a MsgRowsAffected %d", m.Count)
		}
	}
	if r.Err() != context.DeadlineExceeded {
		t.Fatalf("Unexpected error: %v", r.Err())
	}

}

func TestCancelWithNoResults(t *testing.T) {
	conn, logger := open(t)
	defer conn.Close()
	defer logger.StopLogging()

	latency, _ := getLatency(t)
	ctx, cancel := context.WithTimeout(context.Background(), latency+5000*time.Millisecond)
	retmsg := &sqlexp.ReturnMessage{}
	r, err := conn.QueryContext(ctx, `waitfor delay '00:00:15'; select 100`, retmsg)
	if err != nil {
		cancel()
		t.Fatal(err.Error())
	}
	defer r.Close()
	time.Sleep(latency + 100*time.Millisecond)
	cancel()
	active := true
	for active {
		msg := retmsg.Message(ctx)
		t.Logf("Got a message: %s", reflect.TypeOf(msg))
		switch m := msg.(type) {
		case sqlexp.MsgNextResultSet:
			active = r.NextResultSet()
			if active {
				t.Fatal("NextResultSet returned true")
			}
		case sqlexp.MsgNext:
			if r.Next() {
				t.Fatal("Got a successful Next even though the query should been cancelled")
			}
		case sqlexp.MsgRowsAffected:
			t.Fatalf("Got a MsgRowsAffected %d", m.Count)
		}
	}
	if r.Err() != context.Canceled {
		t.Fatalf("Unexpected error: %v", r.Err())
	}
}
