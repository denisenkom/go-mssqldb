// +build go1.9

package mssql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
)

var (
	ErrorEmptyTVPName        = errors.New("TVPName must not be empty")
	ErrorTVPTypeSlice        = errors.New("TVPType must be slice type")
	ErrorTVPTypeSliceIsEmpty = errors.New("TVPType must be slice type")
)

type TVPType struct {
	// TVP param name
	TVPName string
	// TVP scheme name
	TVPScheme string
	// TVP Value. Param must be array
	TVPValue interface{}
}

func (tvp TVPType) check() error {
	if len(tvp.TVPName) == 0 {
		return ErrorEmptyTVPName
	}
	if reflect.TypeOf(tvp.TVPValue).Kind() != reflect.Slice {
		return ErrorTVPTypeSlice
	}
	if reflect.ValueOf(tvp.TVPValue).Len() == 0 {
		return ErrorTVPTypeSliceIsEmpty
	}
	return nil
}

func (tvp TVPType) encode() ([]byte, error) {
	buf := &bytes.Buffer{}
	err := writeBVarChar(buf, "")
	if err != nil {
		return nil, err
	}
	writeBVarChar(buf, tvp.TVPScheme)
	writeBVarChar(buf, tvp.TVPName)
	columnStr, err := tvp.columnTypes()
	if err != nil {
		return nil, err
	}
	binary.Write(buf, binary.LittleEndian, uint16(len(columnStr)))

	for i, column := range columnStr {
		binary.Write(buf, binary.LittleEndian, uint32(column.UserType))
		binary.Write(buf, binary.LittleEndian, uint16(column.Flags))
		writeTypeInfo(buf, &columnStr[i].ti)
		writeBVarChar(buf, "")
	}
	buf.WriteByte(_TVP_END_TOKEN)
	conn := new(Conn)
	conn.sess = new(tdsSession)
	conn.sess.loginAck = loginAckStruct{TDSVersion: verTDS73}
	stmt := &Stmt{
		c: conn,
	}

	val := reflect.ValueOf(tvp.TVPValue)
	for i := 0; i < val.Len(); i++ {
		refStr := reflect.ValueOf(val.Index(i).Interface())
		buf.WriteByte(_TVP_ROW_TOKEN)
		for j := 0; j < refStr.NumField(); j++ {
			tvpVal := refStr.Field(j).Interface()
			if refStr.Field(j).Kind() == reflect.Ptr && reflect.ValueOf(tvpVal).IsNil() {
				switch refStr.Field(j).Type().Elem().Kind() {
				case reflect.Bool:
					binary.Write(buf, binary.LittleEndian, uint8(0))
				default:
					binary.Write(buf, binary.LittleEndian, uint64(_PLP_NULL))
				}
				continue
			}
			if refStr.Field(j).Kind() == reflect.Slice && reflect.ValueOf(tvpVal).IsNil() {
				binary.Write(buf, binary.LittleEndian, uint64(_PLP_NULL))
				continue
			}

			cval, err := convertInputParameter(refStr.Field(j).Interface())
			if err != nil {
				return nil, fmt.Errorf("failed to convert tvp parameter row col: %s", err)
			}
			param, err := stmt.makeParam(cval)
			if err != nil {
				return nil, fmt.Errorf("failed to make tvp parameter row col: %s", err)
			}
			columnStr[j].ti.Writer(buf, param.ti, param.buffer)
		}
	}
	buf.WriteByte(_TVP_END_TOKEN)
	return buf.Bytes(), nil
}

func (tvp TVPType) columnTypes() ([]columnStruct, error) {
	val := reflect.ValueOf(tvp.TVPValue)
	firstRow := val.Index(0).Interface()

	tvpRow := reflect.TypeOf(firstRow)
	columnCount := tvpRow.NumField()
	defaultValues := make([]interface{}, 0, columnCount)

	for i := 0; i < columnCount; i++ {
		if tvpRow.Field(i).Type.Kind() == reflect.Ptr {
			v := reflect.New(tvpRow.Field(i).Type.Elem())
			defaultValues = append(defaultValues, v.Interface())
			continue
		}
		defaultValues = append(defaultValues, reflect.Zero(tvpRow.Field(i).Type).Interface())
	}

	conn := new(Conn)
	conn.sess = new(tdsSession)
	conn.sess.loginAck = loginAckStruct{TDSVersion: verTDS73}
	stmt := &Stmt{
		c: conn,
	}

	columnConfiguration := make([]columnStruct, 0, columnCount)
	for index, val := range defaultValues {
		cval, err := convertInputParameter(val)
		if err != nil {
			return nil, err
		}
		if err != nil {
			return nil, fmt.Errorf("failed to convert tvp parameter row %d col %d: %s", index, val, err)
		}
		param, err := stmt.makeParam(cval)
		if err != nil {
			return nil, err
		}
		column := columnStruct{
			ti: param.ti,
		}
		switch param.ti.TypeId {
		case typeNVarChar, typeBigVarBin:
			column.ti.Size = 0
		}
		columnConfiguration = append(columnConfiguration, column)
	}

	return columnConfiguration, nil
}
