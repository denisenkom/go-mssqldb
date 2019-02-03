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

type TestFieldError struct {
	ErrorValue []*byte
}

type TestFieldsUnsuportedTypes struct {
	ErrorType TestFieldError
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
		{
			name: "TVPValue has wrong type",
			fields: fields{
				TVPValue: []TestFieldError{TestFieldError{}},
			},
			wantErr: true,
		},
		{
			name: "TVPValue has wrong type",
			fields: fields{
				TVPValue: []TestFieldsUnsuportedTypes{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tvp := TVPType{
				TVPTypeName: tt.fields.TVPName,
				TVPScheme:   tt.fields.TVPScheme,
				TVPValue:    tt.fields.TVPValue,
			}
			_, err := tvp.columnTypes()
			if (err != nil) != tt.wantErr {
				t.Errorf("TVPType.columnTypes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestTVPType_check(t *testing.T) {
	type fields struct {
		TVPName   string
		TVPScheme string
		TVPValue  interface{}
	}

	var nullSlice []*string

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name:    "TVPTypeName is nil",
			wantErr: true,
		},
		{
			name: "TVPValue is nil",
			fields: fields{
				TVPName:  "Test",
				TVPValue: nil,
			},
			wantErr: true,
		},
		{
			name: "TVPValue is nil",
			fields: fields{
				TVPName: "Test",
			},
			wantErr: true,
		},
		{
			name: "TVPValue isn't slice",
			fields: fields{
				TVPName:  "Test",
				TVPValue: "",
			},
			wantErr: true,
		},
		{
			name: "TVPValue isn't slice",
			fields: fields{
				TVPName:  "Test",
				TVPValue: 12345,
			},
			wantErr: true,
		},
		{
			name: "TVPValue isn't slice",
			fields: fields{
				TVPName:  "Test",
				TVPValue: nullSlice,
			},
			wantErr: true,
		},
		{
			name: "TVPValue isn't right",
			fields: fields{
				TVPName:  "Test",
				TVPValue: []*fields{},
			},
			wantErr: true,
		},
		{
			name: "TVPValue is right",
			fields: fields{
				TVPName:  "Test",
				TVPValue: []fields{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tvp := TVPType{
				TVPTypeName: tt.fields.TVPName,
				TVPScheme:   tt.fields.TVPScheme,
				TVPValue:    tt.fields.TVPValue,
			}
			if err := tvp.check(); (err != nil) != tt.wantErr {
				t.Errorf("TVPType.check() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func BenchmarkTVPType_check(b *testing.B) {
	type val struct {
		Value string
	}
	tvp := TVPType{
		TVPTypeName: "Test",
		TVPValue:    []val{},
	}
	for i := 0; i < b.N; i++ {
		err := tvp.check()
		if err != nil {
			b.Fail()
		}
	}
}

func BenchmarkColumnTypes(b *testing.B) {
	type str struct {
		bytes      byte
		bytesNull  *byte
		bytesSlice []byte

		int8s      int8
		int8sNull  *int8
		uint8s     uint8
		uint8sNull *uint8

		int16s      int16
		int16sNull  *int16
		uint16s     uint16
		uint16sNull *uint16

		int32s      int32
		int32sNull  *int32
		uint32s     uint32
		uint32sNull *uint32

		int64s      int64
		int64sNull  *int64
		uint64s     uint64
		uint64sNull *uint64

		stringVal     string
		stringValNull *string

		bools     bool
		boolsNull *bool
	}
	wal := make([]str, 100)
	tvp := TVPType{
		TVPTypeName: "Test",
		TVPValue:    wal,
	}
	for i := 0; i < b.N; i++ {
		_, err := tvp.columnTypes()
		if err != nil {
			b.Error(err)
		}
	}
}

func TestTVPType_encode(t *testing.T) {
	type fields struct {
		TVPTypeName string
		TVPScheme   string
		TVPValue    interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{
			name: "TVPValue get error unsupport type",
			fields: fields{
				TVPTypeName: "Test",
				TVPValue:    []TestFieldError{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tvp := TVPType{
				TVPTypeName: tt.fields.TVPTypeName,
				TVPScheme:   tt.fields.TVPScheme,
				TVPValue:    tt.fields.TVPValue,
			}
			_, err := tvp.encode()
			if (err != nil) != tt.wantErr {
				t.Errorf("TVPType.encode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
