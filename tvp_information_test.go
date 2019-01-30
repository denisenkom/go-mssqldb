// +build go1.9

package mssql

import (
	"testing"
	"time"
)

type TestFields struct {
	PBinary       []byte  `db:"p_binary"`
	PVarchar      string  `db:"p_varchar"`
	PNvarchar     *string `db:"p_nvarchar"`
	TimeValue     time.Time
	TimeNullValue *time.Time
}

func TestTVPType_columnTypes(t *testing.T) {
	type fields struct {
		TVPName   string
		TVPScheme string
		TVPValue  interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		want    []columnStruct
		wantErr bool
	}{
		{
			name: "Test Full",
			fields: fields{
				TVPValue: []TestFields{TestFields{}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tvp := TVPType{
				TVPName:   tt.fields.TVPName,
				TVPScheme: tt.fields.TVPScheme,
				TVPValue:  tt.fields.TVPValue,
			}
			_, err := tvp.columnTypes()
			if (err != nil) != tt.wantErr {
				t.Errorf("TVPType.columnTypes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
