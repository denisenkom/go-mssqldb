// +build go1.9

package mssql

import (
	"reflect"
	"testing"
	"time"
)

type TestFields struct {
	PBinary       []byte    `tvp:"p_binary"`
	PVarchar      string    `json:"p_varchar"`
	PNvarchar     *string   `json:"p_nvarchar"`
	TimeValue     time.Time `echo:"-"`
	TimeNullValue *time.Time
}

type TestFieldError struct {
	ErrorValue []*byte
}

type TestFieldsUnsupportedTypes struct {
	ErrorType TestFieldError
}

func TestTVPType_columnTypes(t *testing.T) {
	type customTypeAllFieldsSkipOne struct {
		SkipTest int `tvp:"-"`
	}
	type customTypeAllFieldsSkipMoreOne struct {
		SkipTest  int `tvp:"-"`
		SkipTest1 int `json:"-"`
	}
	type skipWrongField struct {
		SkipTest  int
		SkipTest1 []*byte `json:"skip_test" tvp:"-"`
	}
	type structType struct {
		SkipTest  int               `json:"-" tvp:"test"`
		SkipTest1 []*skipWrongField `json:"any" tvp:"tvp"`
	}
	type skipWithAnotherTagValue struct {
		SkipTest int `json:"-" tvp:"test"`
	}

	type fields struct {
		TVPName  string
		TVPValue interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		want    []columnStruct
		wantErr bool
	}{
		{
			name: "Test Pass",
			fields: fields{
				TVPValue: []TestFields{TestFields{}},
			},
		},
		{
			name: "Value has wrong field type",
			fields: fields{
				TVPValue: []TestFieldError{TestFieldError{}},
			},
			wantErr: true,
		},
		{
			name: "Value has wrong type",
			fields: fields{
				TVPValue: []TestFieldsUnsupportedTypes{},
			},
			wantErr: true,
		},
		{
			name: "Value has wrong type",
			fields: fields{
				TVPValue: []structType{},
			},
			wantErr: true,
		},
		{
			name: "CustomTag all fields are skip, single field",
			fields: fields{
				TVPValue: []customTypeAllFieldsSkipOne{},
			},
			wantErr: true,
		},
		{
			name: "CustomTag all fields are skip, > 1 field",
			fields: fields{
				TVPValue: []customTypeAllFieldsSkipMoreOne{},
			},
			wantErr: true,
		},
		{
			name: "CustomTag all fields are skip wrong field type",
			fields: fields{
				TVPValue: []skipWrongField{},
			},
			wantErr: false,
		},
		{
			name: "CustomTag tag value is not -",
			fields: fields{
				TVPValue: []skipWithAnotherTagValue{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tvp := TVP{
				TypeName: tt.fields.TVPName,
				Value:    tt.fields.TVPValue,
			}
			_, _, err := tvp.columnTypes()
			if (err != nil) != tt.wantErr {
				t.Errorf("TVP.columnTypes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestTVPType_check(t *testing.T) {
	type fields struct {
		TVPName  string
		TVPValue interface{}
	}

	var nullSlice []*string

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name:    "TypeName is nil",
			wantErr: true,
		},
		{
			name: "Value is nil",
			fields: fields{
				TVPName:  "Test",
				TVPValue: nil,
			},
			wantErr: true,
		},
		{
			name: "Value is nil",
			fields: fields{
				TVPName: "Test",
			},
			wantErr: true,
		},
		{
			name: "Value isn't slice",
			fields: fields{
				TVPName:  "Test",
				TVPValue: "",
			},
			wantErr: true,
		},
		{
			name: "Value isn't slice",
			fields: fields{
				TVPName:  "Test",
				TVPValue: 12345,
			},
			wantErr: true,
		},
		{
			name: "Value isn't slice",
			fields: fields{
				TVPName:  "Test",
				TVPValue: nullSlice,
			},
			wantErr: true,
		},
		{
			name: "Value isn't right",
			fields: fields{
				TVPName:  "Test",
				TVPValue: []*fields{},
			},
			wantErr: true,
		},
		{
			name: "Value is right",
			fields: fields{
				TVPName:  "Test",
				TVPValue: []fields{},
			},
			wantErr: false,
		},
		{
			name: "Value is right",
			fields: fields{
				TVPName:  "Test",
				TVPValue: []fields{},
			},
			wantErr: false,
		},
		{
			name: "Value is right",
			fields: fields{
				TVPName:  "[Test]",
				TVPValue: []fields{},
			},
			wantErr: false,
		},
		{
			name: "Value is right",
			fields: fields{
				TVPName:  "[123].[Test]",
				TVPValue: []fields{},
			},
			wantErr: false,
		},
		{
			name: "TVP name is right",
			fields: fields{
				TVPName:  "[123].Test",
				TVPValue: []fields{},
			},
			wantErr: false,
		},
		{
			name: "TVP name is right",
			fields: fields{
				TVPName:  "123.[Test]",
				TVPValue: []fields{},
			},
			wantErr: false,
		},
		{
			name: "TVP name is wrong",
			fields: fields{
				TVPName:  "123.[Test\n]",
				TVPValue: []fields{},
			},
			wantErr: true,
		},
		{
			name: "TVP name is wrong",
			fields: fields{
				TVPName:  "123.[Test].456",
				TVPValue: []fields{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tvp := TVP{
				TypeName: tt.fields.TVPName,
				Value:    tt.fields.TVPValue,
			}
			if err := tvp.check(); (err != nil) != tt.wantErr {
				t.Errorf("TVP.check() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func BenchmarkTVPType_check(b *testing.B) {
	type val struct {
		Value string
	}
	tvp := TVP{
		TypeName: "Test",
		Value:    []val{},
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
	tvp := TVP{
		TypeName: "Test",
		Value:    wal,
	}
	for i := 0; i < b.N; i++ {
		_, _, err := tvp.columnTypes()
		if err != nil {
			b.Error(err)
		}
	}
}

func TestIsSkipField(t *testing.T) {
	type args struct {
		tvpTagValue    string
		isTvpValue     bool
		jsonTagValue   string
		isJsonTagValue bool
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Empty tags",
			want: false,
		},
		{
			name: "tvp is skip",
			want: true,
			args: args{
				isTvpValue:  true,
				tvpTagValue: skipTagValue,
			},
		},
		{
			name: "tvp is any",
			want: false,
			args: args{
				isTvpValue:  true,
				tvpTagValue: "tvp",
			},
		},
		{
			name: "Json is skip",
			want: true,
			args: args{
				isJsonTagValue: true,
				jsonTagValue:   skipTagValue,
			},
		},
		{
			name: "Json is any",
			want: false,
			args: args{
				isJsonTagValue: true,
				jsonTagValue:   "any",
			},
		},
		{
			name: "Json is skip tvp is skip",
			want: true,
			args: args{
				isJsonTagValue: true,
				jsonTagValue:   skipTagValue,
				isTvpValue:     true,
				tvpTagValue:    skipTagValue,
			},
		},
		{
			name: "Json is skip tvp is any",
			want: false,
			args: args{
				isJsonTagValue: true,
				jsonTagValue:   skipTagValue,
				isTvpValue:     true,
				tvpTagValue:    "tvp",
			},
		},
		{
			name: "Json is any tvp is skip",
			want: true,
			args: args{
				isJsonTagValue: true,
				jsonTagValue:   "json",
				isTvpValue:     true,
				tvpTagValue:    skipTagValue,
			},
		},
		{
			name: "Json is any tvp is skip",
			want: false,
			args: args{
				isJsonTagValue: true,
				jsonTagValue:   "json",
				isTvpValue:     true,
				tvpTagValue:    "tvp",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsSkipField(tt.args.tvpTagValue, tt.args.isTvpValue, tt.args.jsonTagValue, tt.args.isJsonTagValue); got != tt.want {
				t.Errorf("IsSkipField() = %v, schema %v", got, tt.want)
			}
		})
	}
}

func Test_getSchemeAndName(t *testing.T) {
	type args struct {
		tvpName string
	}
	tests := []struct {
		name    string
		args    args
		schema  string
		tvpName string
		wantErr bool
	}{
		{
			name:    "Empty object name",
			wantErr: true,
		},
		{
			name:    "Wrong object name",
			wantErr: true,
			args: args{
				tvpName: "1.2.3",
			},
		},
		{
			name:    "Schema+name",
			wantErr: false,
			args: args{
				tvpName: "obj.tvp",
			},
			schema:  "obj",
			tvpName: "tvp",
		},
		{
			name:    "Schema+name",
			wantErr: false,
			args: args{
				tvpName: "[obj].[tvp]",
			},
			schema:  "obj",
			tvpName: "tvp",
		},
		{
			name:    "only name",
			wantErr: false,
			args: args{
				tvpName: "tvp",
			},
			schema:  "",
			tvpName: "tvp",
		},
		{
			name:    "only name",
			wantErr: false,
			args: args{
				tvpName: "[tvp]",
			},
			schema:  "",
			tvpName: "tvp",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, name, err := getSchemeAndName(tt.args.tvpName)
			if (err != nil) != tt.wantErr {
				t.Errorf("getSchemeAndName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if schema != tt.schema {
				t.Errorf("getSchemeAndName() schema = %v, schema %v", schema, tt.schema)
			}
			if name != tt.tvpName {
				t.Errorf("getSchemeAndName() name = %v, schema %v", name, tt.tvpName)
			}
		})
	}
}

func TestTVP_encode(t *testing.T) {
	type fields struct {
		TypeName string
		Value    interface{}
	}
	type args struct {
		schema          string
		name            string
		columnStr       []columnStruct
		tvpFieldIndexes []indexPosition
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name:    "column and indexes are nil",
			wantErr: true,
			args: args{
				tvpFieldIndexes: []indexPosition{
					{
						index:    1,
						position: 1,
					},
					{
						index:    2,
						position: 2,
					},
				},
			},
		},
		{
			name:    "column and indexes are nil",
			wantErr: true,
			args: args{
				tvpFieldIndexes: []indexPosition{
					{
						index:    1,
						position: 1,
					},
					{
						index:    2,
						position: 2,
					},
				},
				columnStr: []columnStruct{columnStruct{}},
			},
		},
		{
			name:    "column and indexes are nil",
			wantErr: true,
			args: args{
				columnStr: []columnStruct{columnStruct{}},
			},
		},
		{
			name:    "column and indexes are nil",
			wantErr: true,
			args: args{
				schema: string(make([]byte, 256)),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tvp := TVP{
				TypeName: tt.fields.TypeName,
				Value:    tt.fields.Value,
			}
			got, err := tvp.encode(tt.args.schema, tt.args.name, tt.args.columnStr, tt.args.tvpFieldIndexes)
			if (err != nil) != tt.wantErr {
				t.Errorf("TVP.encode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TVP.encode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseTvpTag(t *testing.T) {
	type args struct {
		tvpValue string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   uint16
		wantErr bool
	}{
		{
			name:    "empty value",
			args:    args{},
			want:    "",
			want1:   0,
			wantErr: false,
		},
		{
			name: "only name",
			args: args{
				tvpValue: "TVP",
			},
			want:    "TVP",
			want1:   0,
			wantErr: false,
		},
		{
			name: "only position",
			args: args{
				tvpValue: ",1",
			},
			want:    "",
			want1:   1,
			wantErr: false,
		},
		{
			name: "only skip value",
			args: args{
				tvpValue: "-",
			},
			want:    "-",
			want1:   0,
			wantErr: false,
		},
		{
			name: "skip tag and position",
			args: args{
				tvpValue: "-,10",
			},
			want:    "-",
			want1:   10,
			wantErr: false,
		},
		{
			name: "wrong tvp position is string",
			args: args{
				tvpValue: "-,tsc",
			},
			want:    "",
			want1:   0,
			wantErr: true,
		},
		{
			name: "wrong tvp",
			args: args{
				tvpValue: "-,1,123",
			},
			want:    "",
			want1:   0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := parseTvpTag(tt.args.tvpValue)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTvpTag() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseTvpTag() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("parseTvpTag() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_checkPosition(t *testing.T) {
	type args struct {
		check []indexPosition
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "empty value",
			args:    args{},
			wantErr: true,
		},
		{
			name: "empty slice",
			args: args{
				check: []indexPosition{},
			},
			wantErr: true,
		},
		{
			name: "Single value",
			args: args{
				check: []indexPosition{
					{
						index:    1,
						position: 1,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "2 values",
			args: args{
				check: []indexPosition{
					{
						index:    1,
						position: 0,
					},
					{
						index:    2,
						position: 0,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "2 values with wrong positions",
			args: args{
				check: []indexPosition{
					{
						index:    1,
						position: 0,
					},
					{
						index:    2,
						position: 1,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "2 values with same position",
			args: args{
				check: []indexPosition{
					{
						index:    1,
						position: 1,
					},
					{
						index:    2,
						position: 1,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "2 values wrong position",
			args: args{
				check: []indexPosition{
					{
						index:    1,
						position: 1,
					},
					{
						index:    2,
						position: 3,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "2 values wrong position",
			args: args{
				check: []indexPosition{
					{
						index:    1,
						position: 3,
					},
					{
						index:    2,
						position: 1,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "positions are right",
			args: args{
				check: []indexPosition{
					{
						index:    0,
						position: 2,
					},
					{
						index:    2,
						position: 1,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "positions are right",
			args: args{
				check: []indexPosition{
					{
						index:    0,
						position: 1,
					},
					{
						index:    2,
						position: 2,
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := checkPosition(tt.args.check); (err != nil) != tt.wantErr {
				t.Errorf("checkPosition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
