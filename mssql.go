package mssql

import (
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

func init() {
	sql.Register("mssql", &MssqlDriver{})
}

type MssqlDriver struct {
}

type MssqlConn struct {
	sess *tdsSession
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
		case error:
			return token
		}
	}
	return nil
}

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
	buf, err := connect(params)
	if err != nil {
		return nil, err
	}
	return &MssqlConn{buf}, nil
}

func (c *MssqlConn) Close() error {
	return c.sess.buf.transport.Close()
}

type MssqlStmt struct {
	c     *MssqlConn
	query string
}

func (c *MssqlConn) Prepare(query string) (driver.Stmt, error) {
	return &MssqlStmt{c, query}, nil
}

func (s *MssqlStmt) Close() error {
	return nil
}

func (s *MssqlStmt) NumInput() int {
	return -1
}

func (s *MssqlStmt) sendQuery(args []driver.Value) (err error) {
	headers := []headerStruct{
		{hdrtype: dataStmHdrTransDescr,
			data: transDescrHdr{s.c.sess.tranid, 1}.pack()},
	}
	if len(args) == 0 {
		if err = sendSqlBatch72(s.c.sess.buf, s.query, headers); err != nil {
			return
		}
	} else {
		params := make([]Param, len(args)+2)
		decls := make([]string, len(args))
		params[0], err = makeParam(s.query)
		if err != nil {
			return
		}
		for i, val := range args {
			params[i+2], err = makeParam(val)
			if err != nil {
				return
			}
			name := fmt.Sprintf("@p%d", i+1)
			params[i+2].Name = name
			decls[i] = fmt.Sprintf("%s %s", name, makeDecl(params[i+2].ti))
		}
		params[1], err = makeParam(strings.Join(decls, ","))
		if err != nil {
			return
		}
		if err = sendRpc(s.c.sess.buf, headers, Sp_ExecuteSql, 0, params); err != nil {
			return
		}
	}
	return
}

func (s *MssqlStmt) Query(args []driver.Value) (res driver.Rows, err error) {
	if err = s.sendQuery(args); err != nil {
		return
	}
	tokchan := make(chan tokenStruct, 5)
	go processResponse(s.c.sess, tokchan)
	// process metadata
	var cols []string
loop:
	for tok := range tokchan {
		switch token := tok.(type) {
		case doneStruct:
			break loop
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

func (s *MssqlStmt) Exec(args []driver.Value) (res driver.Result, err error) {
	if err = s.sendQuery(args); err != nil {
		return
	}
	tokchan := make(chan tokenStruct, 5)
	go processResponse(s.c.sess, tokchan)
	for token := range tokchan {
		switch token := token.(type) {
		case doneStruct:
			return driver.RowsAffected(token.RowCount), nil
		case error:
			return nil, token
		}
	}
	return driver.ResultNoRows, nil
}

type MssqlRows struct {
	sess    *tdsSession
	nc      int
	cols    []string
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
	for tok := range rc.tokchan {
		switch tokdata := tok.(type) {
		case []columnStruct:
			return streamErrorf("Unexpected token COLMETADATA")
		case []interface{}:
			for i := range dest {
				dest[i] = tokdata[i]
			}
			return nil
		case error:
			return tokdata
		}
	}
	return io.EOF
}

func makeParam(val driver.Value) (res Param, err error) {
	if val == nil {
		res.ti.TypeId = typeNVarChar
		res.buffer = nil
		res.ti.Size = 2
		return
	}
	switch val := val.(type) {
	case int64:
		res.ti.TypeId = typeIntN
		res.buffer = make([]byte, 8)
		res.ti.Size = 8
		binary.LittleEndian.PutUint64(res.buffer, uint64(val))
	case float64:
		res.ti.TypeId = typeFltN
		res.ti.Size = 8
		res.buffer = make([]byte, 8)
		binary.LittleEndian.PutUint64(res.buffer, math.Float64bits(val))
	case []byte:
		res.ti.TypeId = typeBigVarBin
		res.ti.Size = len(val)
		res.buffer = val
	case string:
		res.ti.TypeId = typeNVarChar
		res.buffer = str2ucs2(val)
		res.ti.Size = len(res.buffer)
	case bool:
		res.ti.TypeId = typeBitN
		res.ti.Size = 1
		res.buffer = make([]byte, 1)
		if val {
			res.buffer[0] = 1
		}
	case time.Time:
		res.ti.TypeId = typeDateTimeOffsetN
		panic("time not implemented")
	default:
		err = fmt.Errorf("mssql: unknown type for %T", val)
		return
	}
	return
}
