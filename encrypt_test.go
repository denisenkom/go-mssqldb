package mssql

import (
	"database/sql"
	"strings"
	"testing"
)

func TestBuildQueryParametersForCE(t *testing.T) {
	type test struct {
		name           string
		args           []namedValue
		expectedParams string
		expectedError  string
	}
	var outparam string
	var tests = []test{
		{
			"Single string",
			[]namedValue{
				{Name: "c1", Value: "somestring"},
			},
			`@c1 nvarchar(10)`,
			"",
		},
		{
			"Input and Output params",
			[]namedValue{
				{Name: "", Ordinal: 0, Value: VarChar("somestring")},
				{Name: "c1", Value: int64(5)},
				{Name: "pout", Value: sql.Out{Dest: outparam}},
			},
			`@p0 varchar(10), @c1 bigint, @pout nvarchar(max) output`,
			"",
		},
	}
	s := &Stmt{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := s.buildParametersForColumnEncryption(tc.args)
			if len(tc.expectedError) > 0 {
				if err == nil || strings.Compare(err.Error(), tc.expectedError) != 0 {
					t.Fatalf("buildParametersForColumnEncryption should have failed with %s. Got: %v", tc.expectedError, err)
				}
			} else if err != nil {
				t.Fatalf("buildParametersForColumnEncryption failed with %s", err.Error())
			}
			if strings.Compare(tc.expectedParams, actual) != 0 {
				t.Fatalf("Incorrect parameters. Expected: %s. Got: %s ", tc.expectedParams, actual)
			}
		})
	}
}
func TestSprocQueryForCE(t *testing.T) {
	type test struct {
		name     string
		proc     string
		args     []namedValue
		expected string
	}
	var out int
	tests := []test{
		{
			"Empty args",
			"m]yproc",
			make([]namedValue, 0),
			"EXEC [m]]yproc]",
		},
		{
			"No OUT args",
			"myproc",
			[]namedValue{
				{
					"p1",
					0,
					5,
					nil,
				},
				{
					"@p2",
					0,
					"val",
					nil,
				},
			},
			"EXEC [myproc] @p1=@p1, @p2=@p2",
		},
		{
			"OUT args",
			"myproc",
			[]namedValue{
				{
					"pout",
					0,
					sql.Out{
						Dest: &out,
						In:   false,
					},
					nil,
				},
				{
					"pin",
					1,
					"in",
					nil,
				},
			},
			"EXEC [myproc] @pout=@pout OUTPUT, @pin=@pin",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := buildStoredProcedureStatementForColumnEncryption(tc.proc, tc.args)
			if q != tc.expected {
				t.Fatalf("Incorrect query for %s: %s", tc.name, q)
			}
		})
	}
}
