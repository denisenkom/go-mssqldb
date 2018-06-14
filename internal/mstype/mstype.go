package mstype

import (
	"database/sql/driver"
)

type Param interface {
	SetValue(tp ID, scale uint8, size int) []byte
}

type ExtendedTyper interface {
	CheckNamedValue(nv *driver.NamedValue) error
	MakeParam(val driver.Value, p Param) error
}

type ID uint8

// fixed-length data types
// http://msdn.microsoft.com/en-us/library/dd341171.aspx
const (
	Null     ID = 0x1f
	Int1     ID = 0x30
	Bit      ID = 0x32
	Int2     ID = 0x34
	Int4     ID = 0x38
	DateTim4 ID = 0x3a
	Flt4     ID = 0x3b
	Money    ID = 0x3c
	DateTime ID = 0x3d
	Flt8     ID = 0x3e
	Money4   ID = 0x7a
	Int8     ID = 0x7f
)

// variable-length data types
// http://msdn.microsoft.com/en-us/library/dd358341.aspx
const (
	// byte len types
	Guid            ID = 0x24
	IntN            ID = 0x26
	Decimal         ID = 0x37 // legacy
	Numeric         ID = 0x3f // legacy
	BitN            ID = 0x68
	DecimalN        ID = 0x6a
	NumericN        ID = 0x6c
	FltN            ID = 0x6d
	MoneyN          ID = 0x6e
	DateTimeN       ID = 0x6f
	DateN           ID = 0x28
	TimeN           ID = 0x29
	DateTime2N      ID = 0x2a
	DateTimeOffsetN ID = 0x2b
	Char            ID = 0x2f // legacy
	VarChar         ID = 0x27 // legacy
	Binary          ID = 0x2d // legacy
	VarBinary       ID = 0x25 // legacy

	// short length types
	BigVarBin  ID = 0xa5
	BigVarChar ID = 0xa7
	BigBinary  ID = 0xad
	BigChar    ID = 0xaf
	NVarChar   ID = 0xe7
	NChar      ID = 0xef
	Xml        ID = 0xf1
	Udt        ID = 0xf0

	// long length types
	Text    ID = 0x23
	Image   ID = 0x22
	NText   ID = 0x63
	Variant ID = 0x62
)
