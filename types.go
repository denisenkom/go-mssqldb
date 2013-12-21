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

// variable-length data types
// http://msdn.microsoft.com/en-us/library/dd358341.aspx
const (
    typeGuid = 0x24
    typeIntN = 0x26
    typeDecimal = 0x37  // legacy
    typeNumeric = 0x3f  // legacy
    typeBitN = 0x68
    typeDecimalN = 0x6a
    typeNumericN = 0x6c
    typeFltN = 0x6d
    typeMoneyN = 0x6e
    typeDateTimeN = 0x6f
    typeDateN = 0x28
    typeTimeN = 0x29
    typeDateTime2N = 0x2a
    typeDateTimeOffsetN = 0x2b
    typeChar = 0x2f // legacy
    typeVarChar = 0x27 // legacy
    typeBinary = 0x2d // legacy
    typeVarBinary = 0x25 // legacy

    typeBigVarBin = 0xa5
    typeBigVarChar = 0xa7
    typeBigBinary = 0xad
    typeBigChar = 0xaf
    typeNVarChar = 0xe7
    typeNCharType = 0xef
    typeXml = 0xf1
    typeUdt = 0xf0

    typeText = 0x23
    typeImage = 0x22
    typeNText = 0x63
    typeVariant = 0x62
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


type typeInfoBigVarChar struct {
    size uint16
    collation collation
}

func (t typeInfoBigVarChar)readData(r io.Reader) (value interface{}, err error) {
    var size uint16
    err = binary.Read(r, binary.LittleEndian, &size)
    if err != nil {
        return nil, err
    }
    if size == 0xffff {
        return nil, nil
    }
    buf := make([]byte, size)
    _, err = io.ReadFull(r, buf)
    if err != nil {
        return nil, err
    }
    return string(buf), nil
}

func typeBigVarCharParser(typeid uint8, r io.Reader) (typeInfoIface, error) {
    res := typeInfoBigVarChar{}
    err := binary.Read(r, binary.LittleEndian, &res.size)
    if err != nil {
        return nil, err
    }
    res.collation, err = readCollation(r)
    if err != nil {
        return nil, err
    }
    return &res, nil
}
