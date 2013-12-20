package mssql

import (
    "database/sql"
    "database/sql/driver"
    "io"
//	"math"
//	"math/big"
//	"time"
//	"unsafe"
    "strings"
)

func init() {
    sql.Register("go-mssql", &MssqlDriver{})
}

type MssqlDriver struct {
}

type MssqlConn struct {
    sess *TdsSession
}

type MssqlTx struct {
	c *MssqlConn
}

//func (tx *MssqlTx) Commit() error {
//	_, err := oleutil.CallMethod(tx.c.db, "CommitTrans")
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
//func (tx *MssqlTx) Rollback() error {
//	_, err := oleutil.CallMethod(tx.c.db, "Rollback")
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
//func (c *MssqlConn) exec(cmd string) error {
//	_, err := oleutil.CallMethod(c.db, "Execute", cmd)
//	return err
//}
//
func (c *MssqlConn) Begin() (driver.Tx, error) {
//	_, err := oleutil.CallMethod(c.db, "BeginTrans")
//	if err != nil {
//		return nil, err
//	}
//	return &AdodbTx{c}, nil
    return nil, nil
}

func parseConnectionString(dsn string) (res map[string]string) {
    res = map[string]string{}
    parts := strings.Split(dsn, ";")
    for _, part := range parts {
        if len(part) == 0 {
            continue
        }
        lst := strings.SplitN(part, "=", 2)
        name := strings.ToLower(lst[0])
        if len(name) == 0 {
            continue
        }
        var value string = ""
        if len(lst) > 1 {
            value = lst[1]
        }
        res[name] = value
    }
    return res
}

func (d *MssqlDriver) Open(dsn string) (driver.Conn, error) {
    params := parseConnectionString(dsn)
    buf, err := Connect(params)
    if err != nil {
        return nil, err
    }
    return &MssqlConn{buf}, nil
}

func (c *MssqlConn) Close() error {
    return c.sess.buf.transport.Close()
}

type MssqlStmt struct {
    c  *MssqlConn
    query string
}

func (c *MssqlConn) Prepare(query string) (driver.Stmt, error) {
	return &MssqlStmt{c, query}, nil
}

//func (s *MssqlStmt) Bind(bind []string) error {
//	s.b = bind
//	return nil
//}

func (s *MssqlStmt) Close() error {
//	s.s.Release()
    return nil
}

func (s *MssqlStmt) NumInput() int {
//	if s.b != nil {
//		return len(s.b)
//	}
//	_, err := oleutil.CallMethod(s.ps, "Refresh")
//	if err != nil {
//		return -1
//	}
//	val, err := oleutil.GetProperty(s.ps, "Count")
//	if err != nil {
//		return -1
//	}
//	c := int(val.Val)
//	return c
    return 0
}

//func (s *MssqlStmt) bind(args []driver.Value) error {
//	if s.b != nil {
//		for i, v := range args {
//			var b string = "?"
//			if len(s.b) < i {
//				b = s.b[i]
//			}
//			unknown, err := oleutil.CallMethod(s.s, "CreateParameter", b, 12, 1)
//			if err != nil {
//				return err
//			}
//			param := unknown.ToIDispatch()
//			defer param.Release()
//			_, err = oleutil.PutProperty(param, "Value", v)
//			if err != nil {
//				return err
//			}
//			_, err = oleutil.CallMethod(s.ps, "Append", param)
//			if err != nil {
//				return err
//			}
//		}
//	} else {
//		for i, v := range args {
//			var varval ole.VARIANT
//			varval.VT = ole.VT_I4
//			varval.Val = int64(i)
//			val, err := oleutil.CallMethod(s.ps, "Item", &varval)
//			if err != nil {
//				return err
//			}
//			item := val.ToIDispatch()
//			defer item.Release()
//			_, err = oleutil.PutProperty(item, "Value", v)
//			if err != nil {
//				return err
//			}
//		}
//	}
//	return nil
//}

func (s *MssqlStmt) Query(args []driver.Value) (driver.Rows, error) {
//	if err := s.bind(args); err != nil {
//		return nil, err
//	}
    headers := []headerStruct{
        {hdrtype: dataStmHdrTransDescr,
         data: transDescrHdr{0, 1}.pack()},
    }
    if err := sendSqlBatch72(s.c.sess.buf, s.query, headers); err != nil {
        return nil, err
    }
    return &MssqlRows{sess: s.c.sess}, nil
}

func (s *MssqlStmt) Exec(args []driver.Value) (driver.Result, error) {
//	if err := s.bind(args); err != nil {
//		return nil, err
//	}
//	_, err := oleutil.CallMethod(s.s, "Execute")
//	if err != nil {
//		return nil, err
//	}
    return driver.ResultNoRows, nil
}

type MssqlRows struct {
    sess *TdsSession
//	s    *AdodbStmt
//	rc   *ole.IDispatch
    nc   int
    cols []string
}

func (rc *MssqlRows) Close() error {
//	_, err := oleutil.CallMethod(rc.rc, "Close")
//	if err != nil {
//		return err
//	}
    return nil
}

func (rc *MssqlRows) Columns() []string {
//	if rc.nc != len(rc.cols) {
//		unknown, err := oleutil.GetProperty(rc.rc, "Fields")
//		if err != nil {
//			return []string{}
//		}
//		fields := unknown.ToIDispatch()
//		defer fields.Release()
//		val, err := oleutil.GetProperty(fields, "Count")
//		if err != nil {
//			return []string{}
//		}
//		rc.nc = int(val.Val)
//		rc.cols = make([]string, rc.nc)
//		for i := 0; i < rc.nc; i++ {
//			var varval ole.VARIANT
//			varval.VT = ole.VT_I4
//			varval.Val = int64(i)
//			val, err := oleutil.CallMethod(fields, "Item", &varval)
//			if err != nil {
//				return []string{}
//			}
//			item := val.ToIDispatch()
//			if err != nil {
//				return []string{}
//			}
//			name, err := oleutil.GetProperty(item, "Name")
//			if err != nil {
//				return []string{}
//			}
//			rc.cols[i] = name.ToString()
//			item.Release()
//		}
//	}
//	return rc.cols
    return []string{}
}

func (rc *MssqlRows) Next(dest []driver.Value) error {
//	unknown, err := oleutil.GetProperty(rc.rc, "EOF")
//	if err != nil {
//		return io.EOF
//	}
//	if unknown.Val != 0 {
//		return io.EOF
//	}
//	unknown, err = oleutil.GetProperty(rc.rc, "Fields")
//	if err != nil {
//		return err
//	}
//	fields := unknown.ToIDispatch()
//	defer fields.Release()
//	for i := range dest {
//		var varval ole.VARIANT
//		varval.VT = ole.VT_I4
//		varval.Val = int64(i)
//		val, err := oleutil.CallMethod(fields, "Item", &varval)
//		if err != nil {
//			return err
//		}
//		field := val.ToIDispatch()
//		defer field.Release()
//		typ, err := oleutil.GetProperty(field, "Type")
//		if err != nil {
//			return err
//		}
//		val, err = oleutil.GetProperty(field, "Value")
//		if err != nil {
//			return err
//		}
//		sc, err := oleutil.GetProperty(field, "NumericScale")
//		field.Release()
//		switch typ.Val {
//		case 0: // ADEMPTY
//			dest[i] = nil
//		case 2: // ADSMALLINT
//			dest[i] = int64(int16(val.Val))
//		case 3: // ADINTEGER
//			dest[i] = int64(int32(val.Val))
//		case 4: // ADSINGLE
//			dest[i] = float64(math.Float32frombits(uint32(val.Val)))
//		case 5: // ADDOUBLE
//			dest[i] = math.Float64frombits(uint64(val.Val))
//		case 6: // ADCURRENCY
//			dest[i] = float64(val.Val) / 10000
//		case 7: // ADDATE
//			// see http://blogs.msdn.com/b/ericlippert/archive/2003/09/16/eric-s-complete-guide-to-vt-date.aspx
//			d, t := math.Modf(math.Float64frombits(uint64(val.Val)))
//			t = math.Abs(t)
//			dest[i] = time.Date(1899, 12, 30+int(d), 0, 0, int(t*86400), 0, time.Local)
//		case 8: // ADBSTR
//			dest[i] = val.ToString()
//		case 9: // ADIDISPATCH
//			dest[i] = val.ToIDispatch()
//		case 10: // ADERROR
//			// TODO
//		case 11: // ADBOOLEAN
//			dest[i] = val.Val != 0
//		case 12: // ADVARIANT
//			dest[i] = val
//		case 13: // ADIUNKNOWN
//			dest[i] = val.ToIUnknown()
//		case 14: // ADDECIMAL
//			dest[i] = float64(val.Val)
//		case 16: // ADTINYINT
//			dest[i] = int8(val.Val)
//		case 17: // ADUNSIGNEDTINYINT
//			dest[i] = uint8(val.Val)
//		case 18: // ADUNSIGNEDSMALLINT
//			dest[i] = uint16(val.Val)
//		case 19: // ADUNSIGNEDINT
//			dest[i] = uint32(val.Val)
//		case 20: // ADBIGINT
//			dest[i] = big.NewInt(val.Val)
//		case 21: // ADUNSIGNEDBIGINT
//			// TODO
//		case 72: // ADGUID
//			dest[i] = val.ToString()
//		case 128: // ADBINARY
//			sa := (*ole.SAFEARRAY)(unsafe.Pointer(uintptr(val.Val)))
//			dest[i] = (*[1 << 30]byte)(unsafe.Pointer(uintptr(sa.Data)))[0:sa.Bounds.Elements]
//		case 129: // ADCHAR
//			dest[i] = val.ToString() //uint8(val.Val)
//		case 130: // ADWCHAR
//			dest[i] = val.ToString() //uint16(val.Val)
//		case 131: // ADNUMERIC
//			sub := math.Pow(10, float64(sc.Val))
//			dest[i] = float64(float64(val.Val) / sub)
//		case 132: // ADUSERDEFINED
//			dest[i] = uintptr(val.Val)
//		case 133: // ADDBDATE
//			// see http://blogs.msdn.com/b/ericlippert/archive/2003/09/16/eric-s-complete-guide-to-vt-date.aspx
//			d := math.Float64frombits(uint64(val.Val))
//			dest[i] = time.Date(1899, 12, 30+int(d), 0, 0, 0, 0, time.Local)
//		case 134: // ADDBTIME
//			t := math.Float64frombits(uint64(val.Val))
//			dest[i] = time.Date(0, 1, 1, 0, 0, int(t*86400), 0, time.Local)
//		case 135: // ADDBTIMESTAMP
//			d, t := math.Modf(math.Float64frombits(uint64(val.Val)))
//			t = math.Abs(t)
//			dest[i] = time.Date(1899, 12, 30+int(d), 0, 0, int(t*86400), 0, time.Local)
//		case 136: // ADCHAPTER
//			dest[i] = val.ToString()
//		case 200: // ADVARCHAR
//			dest[i] = val.ToString()
//		case 201: // ADLONGVARCHAR
//			dest[i] = val.ToString()
//		case 202: // ADVARWCHAR
//			dest[i] = val.ToString()
//		case 203: // ADLONGVARWCHAR
//			dest[i] = val.ToString()
//		case 204: // ADVARBINARY
//			// TODO
//		case 205: // ADLONGVARBINARY
//			sa := (*ole.SAFEARRAY)(unsafe.Pointer(uintptr(val.Val)))
//			dest[i] = (*[1 << 30]byte)(unsafe.Pointer(uintptr(sa.Data)))[0:sa.Bounds.Elements]
//		}
//	}
//	_, err = oleutil.CallMethod(rc.rc, "MoveNext")
//	if err != nil {
//		return err
//	}
//	return nil
    return io.EOF
}
