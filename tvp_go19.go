// +build go1.9

package mssql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

const (
	jsonTag      = "json"
	tvpTag       = "tvp"
	skipTagValue = "-"
)

var (
	ErrorEmptyTVPName        = errors.New("TVPTypeName must not be empty")
	ErrorTVPTypeSlice        = errors.New("TVPType must be slice type")
	ErrorTVPTypeSliceIsEmpty = errors.New("TVPType mustn't be null value")
	ErrorTVPSkip             = errors.New("all fields mustn't skip")
	ErrorTVPObjectName       = errors.New("wrong tvp name")
)

//TVPType is driver type, which allows supporting Table Valued Parameters (TVP) in SQL Server
type TVPType struct {
	//TVP param name, mustn't be default value
	TVPTypeName string
	//TVP Value Param must be the slice, mustn't be nil
	TVPValue interface{}
	//TVPCustomTag If the field tag is "-", the field is always omit
	tvpCustomTag string
	//tvp scheme name
	tvpScheme string
	//tvpFieldIndexes
	tvpFieldIndexes []int
}

func (tvp *TVPType) check() error {
	if len(tvp.TVPTypeName) == 0 {
		return ErrorEmptyTVPName
	}
	if !isProc(tvp.TVPTypeName) {
		return ErrorEmptyTVPName
	}
	schema, name, err := getSchemeAndName(tvp.TVPTypeName)
	if err != nil {
		return err
	}
	tvp.TVPTypeName = name
	tvp.tvpScheme = schema

	valueOf := reflect.ValueOf(tvp.TVPValue)
	if valueOf.Kind() != reflect.Slice {
		return ErrorTVPTypeSlice
	}
	if valueOf.IsNil() {
		return ErrorTVPTypeSliceIsEmpty
	}
	if reflect.TypeOf(tvp.TVPValue).Elem().Kind() != reflect.Struct {
		return ErrorTVPTypeSlice
	}
	return nil
}

func (tvp TVPType) encode() ([]byte, error) {
	columnStr, err := tvp.columnTypes()
	if err != nil {
		return nil, err
	}
	preparedBuffer := make([]byte, 0, 20+(10*len(columnStr)))
	buf := bytes.NewBuffer(preparedBuffer)
	err = writeBVarChar(buf, "")
	if err != nil {
		return nil, err
	}
	err = writeBVarChar(buf, tvp.tvpScheme)
	if err != nil {
		return nil, err
	}
	err = writeBVarChar(buf, tvp.TVPTypeName)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, uint16(len(columnStr)))
	if err != nil {
		return nil, err
	}

	for i, column := range columnStr {
		err = binary.Write(buf, binary.LittleEndian, uint32(column.UserType))
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.LittleEndian, uint16(column.Flags))
		if err != nil {
			return nil, err
		}
		err = writeTypeInfo(buf, &columnStr[i].ti)
		if err != nil {
			return nil, err
		}
		err = writeBVarChar(buf, "")
		if err != nil {
			return nil, err
		}
	}
	err = buf.WriteByte(_TVP_END_TOKEN)
	if err != nil {
		return nil, err
	}
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
		for _, j := range tvp.tvpFieldIndexes {
			field := refStr.Field(j)
			tvpVal := field.Interface()
			valOf := reflect.ValueOf(tvpVal)
			elemKind := field.Kind()
			if elemKind == reflect.Ptr && valOf.IsNil() {
				switch tvpVal.(type) {
				case *bool, *time.Time, *int8, *int16, *int32, *int64, *float32, *float64:
					err = binary.Write(buf, binary.LittleEndian, uint8(0))
					if err != nil {
						return nil, err
					}
					continue
				default:
					err = binary.Write(buf, binary.LittleEndian, uint64(_PLP_NULL))
					if err != nil {
						return nil, err
					}
					continue
				}
			}
			if elemKind == reflect.Slice && valOf.IsNil() {
				err = binary.Write(buf, binary.LittleEndian, uint64(_PLP_NULL))
				if err != nil {
					return nil, err
				}
				continue
			}

			cval, err := convertInputParameter(tvpVal)
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
	err = buf.WriteByte(_TVP_END_TOKEN)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (tvp *TVPType) columnTypes() ([]columnStruct, error) {
	val := reflect.ValueOf(tvp.TVPValue)
	var firstRow interface{}
	if val.Len() != 0 {
		firstRow = val.Index(0).Interface()
	} else {
		firstRow = reflect.New(reflect.TypeOf(tvp.TVPValue).Elem()).Elem().Interface()
	}

	tvpRow := reflect.TypeOf(firstRow)
	columnCount := tvpRow.NumField()
	defaultValues := make([]interface{}, 0, columnCount)
	tvp.tvpFieldIndexes = make([]int, 0, columnCount)
	for i := 0; i < columnCount; i++ {
		field := tvpRow.Field(i)
		tvpTagValue, isTvpTag := field.Tag.Lookup(tvpTag)
		jsonTagValue, isJsonTag := field.Tag.Lookup(jsonTag)
		if IsSkipField(tvpTagValue, isTvpTag, jsonTagValue, isJsonTag) {
			continue
		}
		tvp.tvpFieldIndexes = append(tvp.tvpFieldIndexes, i)
		if field.Type.Kind() == reflect.Ptr {
			v := reflect.New(field.Type.Elem())
			defaultValues = append(defaultValues, v.Interface())
			continue
		}
		defaultValues = append(defaultValues, reflect.Zero(field.Type).Interface())
	}

	if columnCount-len(tvp.tvpFieldIndexes) == columnCount {
		return nil, ErrorTVPSkip
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

func IsSkipField(tvpTagValue string, isTvpValue bool, jsonTagValue string, isJsonTagValue bool) bool {
	if !isTvpValue && !isJsonTagValue {
		return false
	} else if isTvpValue && tvpTagValue != skipTagValue {
		return false
	} else if !isTvpValue && isJsonTagValue && jsonTagValue != skipTagValue {
		return false
	}
	return true
}

func getSchemeAndName(tvpName string) (string, string, error) {
	if len(tvpName) == 0 {
		return "", "", ErrorEmptyTVPName
	}
	splitVal := strings.Split(tvpName, ".")
	if len(splitVal) > 2 {
		return "", "", errors.New("wrong tvp name")
	}
	if len(splitVal) == 2 {
		res := make([]string, 2)
		for key, value := range splitVal {
			tmp := strings.Replace(value, "[", "", -1)
			tmp = strings.Replace(tmp, "]", "", -1)
			res[key] = tmp
		}
		return res[0], res[1], nil
	}
	tmp := strings.Replace(splitVal[0], "[", "", -1)
	tmp = strings.Replace(tmp, "]", "", -1)

	return "", tmp, nil
}
