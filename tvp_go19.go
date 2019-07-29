// +build go1.9

package mssql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	jsonTag      = "json"
	tvpTag       = "tvp"
	skipTagValue = "-"
	sqlSeparator = "."
)

var (
	errorEmptyTVPTypeName     = errors.New("TypeName must not be empty")
	errorTypeSlice            = errors.New("TVP must be slice type")
	errorTypeSliceIsEmpty     = errors.New("TVP mustn't be null value")
	errorSkip                 = errors.New("all fields mustn't skip")
	errorObjectName           = errors.New("wrong tvp name")
	errorWrongTyping          = errors.New("the number of elements in columnStr and tvpFieldIndexes do not align")
	errorTvpTagWrong          = errors.New("tvp tag is wrong")
	errorTvpTagPositionWrong  = errors.New("tvp tag tvpPosition is not number")
	errorTVPTagPositionNumber = errors.New("fields must have tvpPosition number")
)

//TVP is driver type, which allows supporting Table Valued Parameters (TVP) in SQL Server
type TVP struct {
	//TypeName mustn't be default value
	TypeName string
	//Value must be the slice, mustn't be nil
	Value interface{}
}

//the model allows keeping the connection between struct field number and TVP field number
type indexPosition struct {
	//struct field number
	fieldIndex int
	//TVP field number
	tvpPosition uint16
}

func (tvp TVP) check() error {
	if len(tvp.TypeName) == 0 {
		return errorEmptyTVPTypeName
	}
	if !isProc(tvp.TypeName) {
		return errorEmptyTVPTypeName
	}
	if sepCount := getCountSQLSeparators(tvp.TypeName); sepCount > 1 {
		return errorObjectName
	}
	valueOf := reflect.ValueOf(tvp.Value)
	if valueOf.Kind() != reflect.Slice {
		return errorTypeSlice
	}
	if valueOf.IsNil() {
		return errorTypeSliceIsEmpty
	}
	if reflect.TypeOf(tvp.Value).Elem().Kind() != reflect.Struct {
		return errorTypeSlice
	}
	return nil
}

func (tvp TVP) encode(schema, name string, columnStr []columnStruct, tvpFieldIndexes []indexPosition) ([]byte, error) {
	if len(columnStr) != len(tvpFieldIndexes) {
		return nil, errorWrongTyping
	}
	isOrdered, errPosition := checkPosition(tvpFieldIndexes)
	if errPosition != nil {
		return nil, errPosition
	}

	preparedBuffer := make([]byte, 0, 20+(10*len(columnStr)))
	buf := bytes.NewBuffer(preparedBuffer)
	err := writeBVarChar(buf, "")
	if err != nil {
		return nil, err
	}

	writeBVarChar(buf, schema)
	writeBVarChar(buf, name)
	binary.Write(buf, binary.LittleEndian, uint16(len(columnStr)))

	if isOrdered {
		for _, fieldIdx := range tvpFieldIndexes {
			binary.Write(buf, binary.LittleEndian, columnStr[fieldIdx.tvpPosition-1].UserType)
			binary.Write(buf, binary.LittleEndian, columnStr[fieldIdx.tvpPosition-1].Flags)
			writeTypeInfo(buf, &columnStr[fieldIdx.tvpPosition-1].ti)
			writeBVarChar(buf, "")
		}
		buf.WriteByte(_TVP_ORDER_TOKEN)
		binary.Write(buf, binary.LittleEndian, uint16(len(tvpFieldIndexes)))
		for _, fieldIdx := range tvpFieldIndexes {
			binary.Write(buf, binary.LittleEndian, fieldIdx.tvpPosition)
		}
	} else {
		for i, column := range columnStr {
			binary.Write(buf, binary.LittleEndian, column.UserType)
			binary.Write(buf, binary.LittleEndian, column.Flags)
			writeTypeInfo(buf, &columnStr[i].ti)
			writeBVarChar(buf, "")
		}
	}
	// The returned error is always nil
	buf.WriteByte(_TVP_END_TOKEN)

	conn := new(Conn)
	conn.sess = new(tdsSession)
	conn.sess.loginAck = loginAckStruct{TDSVersion: verTDS73}
	stmt := &Stmt{
		c: conn,
	}

	val := reflect.ValueOf(tvp.Value)
	for i := 0; i < val.Len(); i++ {
		refStr := reflect.ValueOf(val.Index(i).Interface())
		buf.WriteByte(_TVP_ROW_TOKEN)
		for columnStrIdx, fieldIdx := range tvpFieldIndexes {
			field := refStr.Field(fieldIdx.fieldIndex)
			tvpVal := field.Interface()
			valOf := reflect.ValueOf(tvpVal)
			elemKind := field.Kind()
			if elemKind == reflect.Ptr && valOf.IsNil() {
				switch tvpVal.(type) {
				case *bool, *time.Time, *int8, *int16, *int32, *int64, *float32, *float64, *int:
					binary.Write(buf, binary.LittleEndian, uint8(0))
					continue
				default:
					binary.Write(buf, binary.LittleEndian, uint64(_PLP_NULL))
					continue
				}
			}
			if elemKind == reflect.Slice && valOf.IsNil() {
				binary.Write(buf, binary.LittleEndian, uint64(_PLP_NULL))
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
			columnStr[columnStrIdx].ti.Writer(buf, param.ti, param.buffer)
		}
	}
	buf.WriteByte(_TVP_END_TOKEN)
	return buf.Bytes(), nil
}

func (tvp TVP) columnTypes() ([]columnStruct, []indexPosition, error) {
	val := reflect.ValueOf(tvp.Value)
	var firstRow interface{}
	if val.Len() != 0 {
		firstRow = val.Index(0).Interface()
	} else {
		firstRow = reflect.New(reflect.TypeOf(tvp.Value).Elem()).Elem().Interface()
	}

	tvpRow := reflect.TypeOf(firstRow)
	columnCount := tvpRow.NumField()
	defaultValues := make([]interface{}, 0, columnCount)
	tvpFieldIndexes := make([]indexPosition, 0, columnCount)
	for idx := 0; idx < columnCount; idx++ {
		field := tvpRow.Field(idx)
		tvpTagValue, isTvpTag := field.Tag.Lookup(tvpTag)
		jsonTagValue, isJsonTag := field.Tag.Lookup(jsonTag)
		var positionIndex uint16
		if isTvpTag {
			tvpPart, position, errParse := parseTvpTag(tvpTagValue)
			if errParse != nil {
				return nil, nil, errParse
			}
			tvpTagValue = tvpPart
			positionIndex = position
		}
		if IsSkipField(tvpTagValue, isTvpTag, jsonTagValue, isJsonTag) {
			continue
		}

		tvpFieldIndexes = append(tvpFieldIndexes, indexPosition{
			fieldIndex:  idx,
			tvpPosition: positionIndex,
		})
		if field.Type.Kind() == reflect.Ptr {
			v := reflect.New(field.Type.Elem())
			defaultValues = append(defaultValues, v.Interface())
			continue
		}
		defaultValues = append(defaultValues, reflect.Zero(field.Type).Interface())
	}

	if columnCount-len(tvpFieldIndexes) == columnCount {
		return nil, nil, errorSkip
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
			return nil, nil, fmt.Errorf("failed to convert tvp parameter row %d col %d: %s", index, val, err)
		}
		param, err := stmt.makeParam(cval)
		if err != nil {
			return nil, nil, err
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

	return columnConfiguration, tvpFieldIndexes, nil
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
		return "", "", errorEmptyTVPTypeName
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

func getCountSQLSeparators(str string) int {
	return strings.Count(str, sqlSeparator)
}

func parseTvpTag(tvpValue string) (string, uint16, error) {
	parsedValues := strings.Split(tvpValue, ",")
	if len(parsedValues) > 2 {
		return "", 0, errorTvpTagWrong
	} else if len(parsedValues) == 2 {
		if position, err := strconv.ParseUint(parsedValues[1], 10, 16); err == nil {
			return parsedValues[0], uint16(position), nil
		} else {
			return "", 0, errorTvpTagPositionWrong
		}
	} else {
		return parsedValues[0], 0, nil
	}
}

func checkPosition(check []indexPosition) (bool, error) {
	if check == nil || len(check) == 0 {
		return false, errorWrongTyping
	}
	first := check[0].tvpPosition
	if first == 0 {
		for idx := range check {
			if first == 0 && check[idx].tvpPosition != first {
				return false, errorTVPTagPositionNumber
			}
		}
		return false, nil
	}

	sortByPosition(check)
	for pos := 1; pos <= len(check); pos++ {
		if check[pos-1].tvpPosition != uint16(pos) {
			return false, errorTVPTagPositionNumber
		}
	}
	sortByIndex(check)
	return true, nil
}

func sortByPosition(check []indexPosition) {
	sort.Slice(check, func(i, j int) bool {
		return check[i].tvpPosition < check[j].tvpPosition
	})
}

func sortByIndex(check []indexPosition) {
	sort.Slice(check, func(i, j int) bool {
		return check[i].fieldIndex < check[j].fieldIndex
	})
}
