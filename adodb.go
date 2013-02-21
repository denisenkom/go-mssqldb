package adodb

import (
	"database/sql"
	"database/sql/driver"
	"github.com/mattn/go-ole"
	"github.com/mattn/go-ole/oleutil"
	"io"
	"math/big"
	"time"
	"unsafe"
)

func init() {
	ole.CoInitialize(0)
	sql.Register("adodb", &AdodbDriver{})
}

type AdodbDriver struct {

}

type AdodbConn struct {
	db *ole.IDispatch
}

type AdodbTx struct {
	c *AdodbConn
}

func (tx *AdodbTx) Commit() error {
	_, err := oleutil.CallMethod(tx.c.db, "CommitTrans")
	if err != nil {
		return err
	}
	return nil
}

func (tx *AdodbTx) Rollback() error {
	_, err := oleutil.CallMethod(tx.c.db, "Rollback")
	if err != nil {
		return err
	}
	return nil
}

func (c *AdodbConn) exec(cmd string) error {
	_, err := oleutil.CallMethod(c.db, "Execute", cmd)
	return err
}

func (c *AdodbConn) Begin() (driver.Tx, error) {
	_, err := oleutil.CallMethod(c.db, "BeginTrans")
	if err != nil {
		return nil, err
	}
	return &AdodbTx{c}, nil
}

func (d *AdodbDriver) Open(dsn string) (driver.Conn, error) {
	unknown, err := oleutil.CreateObject("ADODB.Connection")
	if err != nil {
		return nil, err
	}
	db, err := unknown.QueryInterface(ole.IID_IDispatch)
	if err != nil {
		return nil, err
	}
	_, err = oleutil.CallMethod(db, "Open", dsn)
	if err != nil {
		return nil, err
	}
	return &AdodbConn{db}, nil
}

func (c *AdodbConn) Close() error {
	_, err := oleutil.CallMethod(c.db, "Close")
	if err != nil {
		return err
	}
	c.db = nil
	return nil
}

type AdodbStmt struct {
	c  *AdodbConn
	s  *ole.IDispatch
	ps *ole.IDispatch
	b  []string
}

func (c *AdodbConn) Prepare(query string) (driver.Stmt, error) {
	unknown, err := oleutil.CreateObject("ADODB.Command")
	if err != nil {
		return nil, err
	}
	s, err := unknown.QueryInterface(ole.IID_IDispatch)
	if err != nil {
		return nil, err
	}
	_, err = oleutil.PutProperty(s, "ActiveConnection", c.db)
	if err != nil {
		return nil, err
	}
	_, err = oleutil.PutProperty(s, "CommandText", query)
	if err != nil {
		return nil, err
	}
	_, err = oleutil.PutProperty(s, "CommandType", 1)
	if err != nil {
		return nil, err
	}
	_, err = oleutil.PutProperty(s, "Prepared", true)
	if err != nil {
		return nil, err
	}
	val, err := oleutil.GetProperty(s, "Parameters")
	if err != nil {
		return nil, err
	}
	return &AdodbStmt{c, s, val.ToIDispatch(), nil}, nil
}

func (s *AdodbStmt) Bind(bind []string) error {
	s.b = bind
	return nil
}

func (s *AdodbStmt) Close() error {
	s.s.Release()
	return nil
}

func (s *AdodbStmt) NumInput() int {
	if s.b != nil {
		return len(s.b)
	}
	_, err := oleutil.CallMethod(s.ps, "Refresh")
	if err != nil {
		return -1
	}
	val, err := oleutil.GetProperty(s.ps, "Count")
	if err != nil {
		return -1
	}
	c := int(val.Val)
	return c
}

func (s *AdodbStmt) bind(args []driver.Value) error {
	if s.b != nil {
		for i, v := range args {
			var b string = "?"
			if len(s.b) < i {
				b = s.b[i]
			}
			unknown, err := oleutil.CallMethod(s.s, "CreateParameter", b, 12, 1)
			if err != nil {
				return err
			}
			param := unknown.ToIDispatch()
			defer param.Release()
			_, err = oleutil.PutProperty(param, "Value", v)
			if err != nil {
				return err
			}
			_, err = oleutil.CallMethod(s.ps, "Append", param)
			if err != nil {
				return err
			}
		}
	} else {
		for i, v := range args {
			var varval ole.VARIANT
			varval.VT = ole.VT_I4
			varval.Val = int64(i)
			val, err := oleutil.CallMethod(s.ps, "Item", &varval)
			if err != nil {
				return err
			}
			item := val.ToIDispatch()
			defer item.Release()
			_, err = oleutil.PutProperty(item, "Value", v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *AdodbStmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := s.bind(args); err != nil {
		return nil, err
	}
	rc, err := oleutil.CallMethod(s.s, "Execute")
	if err != nil {
		return nil, err
	}
	return &AdodbRows{s, rc.ToIDispatch(), -1, nil}, nil
}

func (s *AdodbStmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.bind(args); err != nil {
		return nil, err
	}
	_, err := oleutil.CallMethod(s.s, "Execute")
	if err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

type AdodbRows struct {
	s    *AdodbStmt
	rc   *ole.IDispatch
	nc   int
	cols []string
}

func (rc *AdodbRows) Close() error {
	_, err := oleutil.CallMethod(rc.rc, "Close")
	if err != nil {
		return err
	}
	return nil
}

func (rc *AdodbRows) Columns() []string {
	if rc.nc != len(rc.cols) {
		unknown, err := oleutil.GetProperty(rc.rc, "Fields")
		if err != nil {
			return []string{}
		}
		fields := unknown.ToIDispatch()
		defer fields.Release()
		val, err := oleutil.GetProperty(fields, "Count")
		if err != nil {
			return []string{}
		}
		rc.nc = int(val.Val)
		rc.cols = make([]string, rc.nc)
		for i := 0; i < rc.nc; i++ {
			var varval ole.VARIANT
			varval.VT = ole.VT_I4
			varval.Val = int64(i)
			val, err := oleutil.CallMethod(fields, "Item", &varval)
			if err != nil {
				return []string{}
			}
			item := val.ToIDispatch()
			if err != nil {
				return []string{}
			}
			name, err := oleutil.GetProperty(item, "Name")
			if err != nil {
				return []string{}
			}
			rc.cols[i] = name.ToString()
			item.Release()
		}
	}
	return rc.cols
}

func (rc *AdodbRows) Next(dest []driver.Value) error {
	unknown, err := oleutil.GetProperty(rc.rc, "EOF")
	if err != nil {
		return io.EOF
	}
	if unknown.Val != 0 {
		return io.EOF
	}
	unknown, err = oleutil.GetProperty(rc.rc, "Fields")
	if err != nil {
		return err
	}
	fields := unknown.ToIDispatch()
	defer fields.Release()
	for i := range dest {
		var varval ole.VARIANT
		varval.VT = ole.VT_I4
		varval.Val = int64(i)
		val, err := oleutil.CallMethod(fields, "Item", &varval)
		if err != nil {
			return err
		}
		field := val.ToIDispatch()
		defer field.Release()
		typ, err := oleutil.GetProperty(field, "Type")
		if err != nil {
			return err
		}
		val, err = oleutil.GetProperty(field, "Value")
		if err != nil {
			return err
		}
		field.Release()
		switch typ.Val {
		case 0: // ADEMPTY
			// TODO
		case 2: // ADSMALLINT
			dest[i] = int16(val.Val)
		case 3: // ADINTEGER
			dest[i] = int32(val.Val)
		case 4: // ADSINGLE
			dest[i] = float32(val.Val)
		case 5: // ADDOUBLE
			dest[i] = val.Val
		case 6: // ADCURRENCY
			dest[i] = float64(val.Val)
		case 7: // ADDATE
			// TODO
		case 8: // ADBSTR
			dest[i] = val.ToString()
		case 9: // ADIDISPATCH
			dest[i] = val.ToIDispatch()
		case 10: // ADERROR
			// TODO
		case 11: // ADBOOLEAN
			if val.Val != 0 {
				dest[i] = true
			} else {
				dest[i] = false
			}
		case 12: // ADVARIANT
			dest[i] = val
		case 13: // ADIUNKNOWN
			dest[i] = val.ToIUnknown()
		case 14: // ADDECIMAL
			dest[i] = float64(val.Val)
		case 16: // ADTINYINT
			dest[i] = int8(val.Val)
		case 17: // ADUNSIGNEDTINYINT
			dest[i] = uint8(val.Val)
		case 18: // ADUNSIGNEDSMALLINT
			dest[i] = uint16(val.Val)
		case 19: // ADUNSIGNEDINT
			dest[i] = uint32(val.Val)
		case 20: // ADBIGINT
			dest[i] = big.NewInt(val.Val)
		case 21: // ADUNSIGNEDBIGINT
			// TODO
		case 72: // ADGUID
			// TODO
		case 128: // ADBINARY
			sa := *(**ole.SAFEARRAY)(unsafe.Pointer(&val.Val))
			dest[i] = (*[1 << 30]byte)(unsafe.Pointer(uintptr(sa.PvData)))[0:sa.CbElements]
		case 129: // ADCHAR
			dest[i] = val.ToString()//uint8(val.Val)
		case 130: // ADWCHAR
			dest[i] = val.ToString()//uint16(val.Val)
		case 131: // ADNUMERIC
			dest[i] = val.Val
		case 132: // ADUSERDEFINED
			dest[i] = uintptr(val.Val)
		case 133: // ADDBDATE
			dest[i] = time.Unix(0, val.Val).UTC()
		case 134: // ADDBTIME
			dest[i] = time.Unix(0, val.Val).UTC()
		case 135: // ADDBTIMESTAMP
			dest[i] = time.Unix(0, val.Val).UTC()
		case 136: // ADCHAPTER
			dest[i] = val.ToString()
		case 200: // ADVARCHAR
			dest[i] = val.ToString()
		case 201: // ADLONGVARCHAR
			dest[i] = val.ToString()
		case 202: // ADVARWCHAR
			dest[i] = val.ToString()
		case 203: // ADLONGVARWCHAR
			dest[i] = val.ToString()
		case 204: // ADVARBINARY
			// TODO
		case 205: // ADLONGVARBINARY
			// TODO
		}
	}
	_, err = oleutil.CallMethod(rc.rc, "MoveNext")
	if err != nil {
		return err
	}
	return nil
}
