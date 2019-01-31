// +build go1.9

package mssql

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type tvpWrapper interface {
	TVP() (typeName string, exampleRow []interface{}, rows [][]interface{})
}

type TableValuedParam struct {
	TypeName   string
	Rows       [][]interface{}
	ExampleRow []interface{}
}

func (p *TableValuedParam) encode(tvpParam param) ([]byte, error) {

	var params [][]param
	var columns []columnStruct

	stmt := &Stmt{}

	// Make rows of parameters from the rows of data
	for rowI, row := range p.Rows {
		var rowParams []param
		for colI, val := range row {
			cval, err := convertInputParameter(val)
			if err != nil {
				return nil, fmt.Errorf("failed to convert tvp parameter row %d col %d: %s", rowI, colI, err)
			}
			param, err := stmt.makeParam(cval)
			if err != nil {
				return nil, fmt.Errorf("failed to make tvp parameter row %d col %d: %s", rowI, colI, err)
			}
			rowParams = append(rowParams, param)
		}
		params = append(params, rowParams)
	}

	// Use the first row for the column (without size?) (TODO: Check this...)
	for i, col := range p.ExampleRow {

		cval, err := convertInputParameter(col)
		param, err := stmt.makeParam(cval)
		if err != nil {
			return nil, fmt.Errorf("failed to make example row parameter col %d: %s", i, err)
		}
		var c columnStruct
		c.ti = param.ti
		switch param.ti.TypeId { // TODO: This seems ugly. better way?
		case typeNVarChar, typeBigVarBin:
			c.ti.Size = 0
		}

		columns = append(columns, c)
	}

	buf := &bytes.Buffer{}

	// w.write_b_varchar("")  # db_name, should be empty
	writeBVarChar(buf, "")

	// w.write_b_varchar(self._table_type.typ_schema)
	writeBVarChar(buf, tvpParam.ti.UdtInfo.SchemaName)

	// w.write_b_varchar(self._table_type.typ_name)
	writeBVarChar(buf, tvpParam.ti.UdtInfo.TypeName)

	// w.put_usmallint(len(columns))
	binary.Write(buf, binary.LittleEndian, uint16(len(columns)))

	//for i, column in enumerate(columns):
	for i, column := range columns {

		// w.put_uint(column.column_usertype)
		binary.Write(buf, binary.LittleEndian, uint32(column.UserType))

		// w.put_usmallint(column.flags)
		binary.Write(buf, binary.LittleEndian, uint16(column.Flags))

		// # TYPE_INFO structure: https://msdn.microsoft.com/en-us/library/dd358284.aspx

		// serializer = self._columns_serializers[i]
		// type_id = serializer.type
		// w.put_byte(type_id) // ES: Appears to be done by writeTypeInfo
		// serializer.write_info(w)
		writeTypeInfo(buf, &columns[i].ti)

		// w.write_b_varchar('')  # ColName, must be empty in TVP according to spec
		writeBVarChar(buf, "")
	}

	// # here can optionally send TVP_ORDER_UNIQUE and TVP_COLUMN_ORDERING
	// # https://msdn.microsoft.com/en-us/library/dd305261.aspx

	// # terminating optional metadata
	// w.put_byte(tds_base.TVP_END_TOKEN)
	buf.WriteByte(_TVP_END_TOKEN)

	fmt.Println(buf.Bytes())

	// # now sending rows using TVP_ROW
	// # https://msdn.microsoft.com/en-us/library/dd305261.aspx
	// if val.rows:
	// 	for row in val.rows:
	for _, row := range params {
		// 		w.put_byte(tds_base.TVP_ROW_TOKEN)
		buf.WriteByte(_TVP_ROW_TOKEN)

		// 		for i, col in enumerate(self._table_type.columns):
		for i, param := range row {

			// 			if not col.flags & tds_base.TVP_COLUMN_DEFAULT_FLAG:
			// 				self._columns_serializers[i].write(w, row[i])
			//if !p.columns[i].Flags&_TVP_COLUMN_DEFAULT_FLAG != 0 { // TODO: Is this needed?
			columns[i].ti.Writer(buf, param.ti, param.buffer)
			//}
		}
	}

	// # terminating rows
	// w.put_byte(tds_base.TVP_END_TOKEN)
	buf.WriteByte(_TVP_END_TOKEN)

	fmt.Println(buf.Bytes())

	return buf.Bytes(), nil
}
