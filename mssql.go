package mssql

import (
    "io"
    "database/sql"
    "database/sql/driver"
    "encoding/binary"
    "math"
//	"math/big"
    "time"
//	"unsafe"
    "strings"
    "fmt"
)

func init() {
    sql.Register("go-mssql", &MssqlDriver{})
}

type MssqlDriver struct {
}

type MssqlConn struct {
    sess *TdsSession
}

func (c *MssqlConn) Commit() error {
    headers := []headerStruct{
        {hdrtype: dataStmHdrTransDescr,
         data: transDescrHdr{c.sess.tranid, 1}.pack()},
    }
    if err := sendCommitXact(c.sess.buf, headers, "", 0, 0, ""); err != nil {
        return err
    }

    tokchan := make(chan tokenStruct, 5)
    go processResponse(c.sess, tokchan)
    for tok := range tokchan {
        switch token := tok.(type) {
        case doneStruct:
            if token.Status & doneError != 0 {
                return c.sess.messages[0]
            }
            break
        case error:
            return token
        }
    }
    return nil
}

func (c *MssqlConn) Rollback() error {
    headers := []headerStruct{
        {hdrtype: dataStmHdrTransDescr,
         data: transDescrHdr{c.sess.tranid, 1}.pack()},
    }
    if err := sendRollbackXact(c.sess.buf, headers, "", 0, 0, ""); err != nil {
        return err
    }

    tokchan := make(chan tokenStruct, 5)
    go processResponse(c.sess, tokchan)
    for tok := range tokchan {
        switch token := tok.(type) {
        case doneStruct:
            if token.Status & doneError != 0 {
                return c.sess.messages[0]
            }
            break
        case error:
            return token
        }
    }
    return nil
}

//func (c *MssqlConn) exec(cmd string) error {
//	_, err := oleutil.CallMethod(c.db, "Execute", cmd)
//	return err
//}
//
func (c *MssqlConn) Begin() (driver.Tx, error) {
    headers := []headerStruct{
        {hdrtype: dataStmHdrTransDescr,
         data: transDescrHdr{0, 1}.pack()},
    }
    if err := sendBeginXact(c.sess.buf, headers, 0, ""); err != nil {
        return nil, err
    }
    tokchan := make(chan tokenStruct, 5)
    go processResponse(c.sess, tokchan)
    for tok := range tokchan {
        switch token := tok.(type) {
        case doneStruct:
            if token.Status & doneError != 0 {
                return nil, c.sess.messages[0]
            }
            break
        case error:
            return nil, token
        }
    }
    // successful BEGINXACT request will return sess.tranid
    // for started transaction
    return c, nil
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
    return -1
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

func (s *MssqlStmt) Query(args []driver.Value) (res driver.Rows, err error) {
    headers := []headerStruct{
        {hdrtype: dataStmHdrTransDescr,
         data: transDescrHdr{0, 1}.pack()},
    }
    if len(args) == 0 {
        if err = sendSqlBatch72(s.c.sess.buf, s.query, headers); err != nil {
            return
        }
    } else {
        params := make([]Param, len(args) + 2)
        decls := make([]string, len(args))
        params[0], err = makeParam(s.query); if err != nil {
            return
        }
        for i, val := range args {
            params[i + 2], err = makeParam(val); if err != nil {
                return
            }
            name := fmt.Sprintf("@p%d", i + 1)
            params[i + 2].Name = name
            decls[i] = fmt.Sprintf("%s %s", name, makeDecl(params[i + 2].ti))
        }
        params[1], err = makeParam(strings.Join(decls, ",")); if err != nil {
            return
        }
        if err = sendRpc(s.c.sess.buf, headers, Sp_ExecuteSql, 0, params); err != nil {
            return
        }
    }
    tokchan := make(chan tokenStruct, 5)
    go processResponse(s.c.sess, tokchan)
    // process metadata
    var cols []string
    loop:
    for tok := range tokchan {
        switch token := tok.(type) {
        case doneStruct:
            if token.Status & doneError != 0 {
                return nil, s.c.sess.messages[0]
            }
            return nil, nil
        case []columnStruct:
            cols = make([]string, len(token))
            for i, col := range token {
                cols[i] = col.ColName
            }
            break loop
        case error:
            return nil, token
        }
    }
    return &MssqlRows{sess: s.c.sess, tokchan: tokchan, cols: cols}, nil
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
    nc   int
    cols []string
    tokchan chan tokenStruct
}

func (rc *MssqlRows) Close() error {
    rc.tokchan = nil
    return nil
}

func (rc *MssqlRows) Columns() (res []string) {
    return rc.cols
}

func (rc *MssqlRows) Next(dest []driver.Value) (err error) {
    if rc.tokchan == nil {
        return io.EOF
    }
    for tok := range rc.tokchan {
        switch tokdata := tok.(type) {
        case doneStruct:
            rc.tokchan = nil
            if tokdata.Status & doneError != 0 {
                return rc.sess.messages[0]
            }
            return io.EOF
        case []columnStruct:
            return streamErrorf("Unexpected token COLMETADATA")
        case []interface{}:
            for i := range dest {
                dest[i] = tokdata[i]
            }
            return nil
        case error:
            rc.tokchan = nil
            return tokdata
        }
    }
    return nil
}


func makeParam(val driver.Value) (res Param, err error) {
    if val == nil {
        res.ti.TypeId = typeNChar
        res.buffer = nil
        res.ti.Size = 2
        res.ti.Writer = writeShortLenType
        return
    }
    switch val := val.(type) {
    case int64:
        res.ti.TypeId = typeIntN
        res.buffer = make([]byte, 8)
        res.ti.Size = 8
        binary.LittleEndian.PutUint64(res.buffer, uint64(val))
        res.ti.Writer = writeByteLenType
    case float32:
        res.ti.TypeId = typeFltN
        res.ti.Size = 4
        res.buffer = make([]byte, 4)
        binary.LittleEndian.PutUint32(res.buffer, math.Float32bits(val))
        res.ti.Writer = writeByteLenType
    case float64:
        res.ti.TypeId = typeFltN
        res.ti.Size = 8
        res.buffer = make([]byte, 8)
        binary.LittleEndian.PutUint64(res.buffer, math.Float64bits(val))
        res.ti.Writer = writeByteLenType
    case []byte:
        res.ti.TypeId = typeBigBinary
        res.ti.Size = len(val)
        res.buffer = val
        res.ti.Writer = writeShortLenType
    case string:
        res.ti.TypeId = typeNChar
        res.buffer = str2ucs2(val)
        res.ti.Size = len(res.buffer)
        res.ti.Writer = writeShortLenType
    case bool:
        res.ti.TypeId = typeBitN
        res.ti.Size = 1
        res.buffer = make([]byte, 1)
        if val {
            res.buffer[0] = 1
        }
        res.ti.Writer = writeByteLenType
    case time.Time:
        res.ti.TypeId = typeDateTimeOffsetN
        panic("time not implemented")
    default:
        err = fmt.Errorf("mssql: unknown type for %T", val)
        return
    }
    return
}
