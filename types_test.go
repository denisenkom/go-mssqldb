package mssql

import (
	"reflect"
	"testing"
	"time"

	"github.com/denisenkom/go-mssqldb/internal/mstype"
)

func TestMakeGoLangScanType(t *testing.T) {
	if (reflect.TypeOf(int64(0)) != makeGoLangScanType(typeInfo{TypeID: mstype.Int8})) {
		t.Errorf("invalid type returned for typeDateTime")
	}
	if (reflect.TypeOf(float64(0)) != makeGoLangScanType(typeInfo{TypeID: mstype.Flt4})) {
		t.Errorf("invalid type returned for typeDateTime")
	}
	if (reflect.TypeOf(float64(0)) != makeGoLangScanType(typeInfo{TypeID: mstype.Flt8})) {
		t.Errorf("invalid type returned for typeDateTime")
	}
	if (reflect.TypeOf("") != makeGoLangScanType(typeInfo{TypeID: mstype.VarChar})) {
		t.Errorf("invalid type returned for typeDateTime")
	}
	if (reflect.TypeOf(time.Time{}) != makeGoLangScanType(typeInfo{TypeID: mstype.DateTime})) {
		t.Errorf("invalid type returned for typeDateTime")
	}
	if (reflect.TypeOf(time.Time{}) != makeGoLangScanType(typeInfo{TypeID: mstype.DateTim4})) {
		t.Errorf("invalid type returned for typeDateTim4")
	}
	if (reflect.TypeOf(int64(0)) != makeGoLangScanType(typeInfo{TypeID: mstype.Int1})) {
		t.Errorf("invalid type returned for typeInt1")
	}
	if (reflect.TypeOf(int64(0)) != makeGoLangScanType(typeInfo{TypeID: mstype.Int2})) {
		t.Errorf("invalid type returned for typeInt2")
	}
	if (reflect.TypeOf(int64(0)) != makeGoLangScanType(typeInfo{TypeID: mstype.Int4})) {
		t.Errorf("invalid type returned for typeInt4")
	}
	if (reflect.TypeOf(int64(0)) != makeGoLangScanType(typeInfo{TypeID: mstype.IntN, Size: 4})) {
		t.Errorf("invalid type returned for typeIntN")
	}
	if (reflect.TypeOf([]byte{}) != makeGoLangScanType(typeInfo{TypeID: mstype.Money, Size: 8})) {
		t.Errorf("invalid type returned for typeIntN")
	}
}

func TestMakeGoLangTypeName(t *testing.T) {
	defer handlePanic(t)

	tests := []struct {
		typeName   string
		typeString string
		typeID     mstype.ID
	}{
		{"typeDateTime", "DATETIME", mstype.DateTime},
		{"typeDateTim4", "SMALLDATETIME", mstype.DateTim4},
		{"typeBigBinary", "BINARY", mstype.BigBinary},
		//TODO: Add other supported types
	}

	for _, tt := range tests {
		if makeGoLangTypeName(typeInfo{TypeID: tt.typeID}) != tt.typeString {
			t.Errorf("invalid type name returned for %s", tt.typeName)
		}
	}
}

func TestMakeGoLangTypeLength(t *testing.T) {
	defer handlePanic(t)

	tests := []struct {
		typeName   string
		typeVarLen bool
		typeLen    int64
		typeID     mstype.ID
	}{
		{"typeDateTime", false, 0, mstype.DateTime},
		{"typeDateTim4", false, 0, mstype.DateTim4},
		{"typeBigBinary", false, 0, mstype.BigBinary},
		//TODO: Add other supported types
	}

	for _, tt := range tests {
		n, v := makeGoLangTypeLength(typeInfo{TypeID: tt.typeID})
		if v != tt.typeVarLen {
			t.Errorf("invalid type length variability returned for %s", tt.typeName)
		}
		if n != tt.typeLen {
			t.Errorf("invalid type length returned for %s", tt.typeName)
		}
	}
}

func TestMakeGoLangTypePrecisionScale(t *testing.T) {
	defer handlePanic(t)

	tests := []struct {
		typeName   string
		typeID     mstype.ID
		typeVarLen bool
		typePrec   int64
		typeScale  int64
	}{
		{"typeDateTime", mstype.DateTime, false, 0, 0},
		{"typeDateTim4", mstype.DateTim4, false, 0, 0},
		{"typeBigBinary", mstype.BigBinary, false, 0, 0},
		//TODO: Add other supported types
	}

	for _, tt := range tests {
		prec, scale, varLen := makeGoLangTypePrecisionScale(typeInfo{TypeID: tt.typeID})
		if varLen != tt.typeVarLen {
			t.Errorf("invalid type length variability returned for %s", tt.typeName)
		}
		if prec != tt.typePrec || scale != tt.typeScale {
			t.Errorf("invalid type precision and/or scale returned for %s", tt.typeName)
		}
	}
}

func handlePanic(t *testing.T) {
	if r := recover(); r != nil {
		t.Errorf("recovered panic")
	}
}
