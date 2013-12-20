package mssql

import (
    "io"
    "encoding/binary"
)


// fixed-length data types
// http://msdn.microsoft.com/en-us/library/dd341171.aspx
const (
    typeNull = 0x1f
    typeInt1 = 0x30
    typeBit = 0x32
    typeInt2 = 0x34
    typeInt4 = 0x38
    typeDateTim4 = 0x3a
    typeFlt4 = 0x3b
    typeMoney = 0x3c
    typeDateTime = 0x3d
    typeFlt8 = 0x3e
    typeMoney4 = 0x7a
    typeInt8 = 0x7f
)


type typeInfoIface interface {
    readData(r io.Reader) (value interface{}, err error)
}

type typeParser func(typeid uint8, r io.Reader) (typeInfoIface, error)


type typeInfoInt4 struct {}

var typeInfoInt4Instance = typeInfoInt4{}

func (t typeInfoInt4)readData(r io.Reader) (value interface{}, err error) {
    var ivalue int32
    err = binary.Read(r, binary.LittleEndian, &ivalue)
    return ivalue, err
}

func typeInt4Parser(typeid uint8, r io.Reader) (res typeInfoIface, err error) {
    return typeInfoInt4Instance, nil
}
