package mssql

import (
	"database/sql/driver"
	"errors"
	"strings"
)

type copyin struct {
	cn       *MssqlConn
	bulkcopy *MssqlBulk
	closed   bool
}

func (c *MssqlConn) prepareCopyIn(query string) (_ driver.Stmt, err error) {

	query = query[11:]
	info := strings.Split(query, "\u001f")

	table := info[0]
	columns := info[1:]

	bulkcopy := c.CreateBulk(table, columns)
	ci := &copyin{
		cn:       c,
		bulkcopy: bulkcopy,
	}

	return ci, nil
}

func CopyIn(table string, columns ...string) string {

	info := table + "\u001f" + strings.Join(columns, "\u001f")
	stmt := "INSERTBULK " + string(info)

	return stmt
}

func (ci *copyin) NumInput() int {
	return -1
}

func (ci *copyin) Query(v []driver.Value) (r driver.Rows, err error) {
	return nil, errors.New("ErrNotSupported")
}

func (ci *copyin) Exec(v []driver.Value) (r driver.Result, err error) {
	if ci.closed {
		return nil, errors.New("errCopyInClosed")
	}

	if len(v) == 0 {
		rowCount, err := ci.bulkcopy.Done()
		ci.closed = true
		return driver.RowsAffected(rowCount), err
	}

	t := make([]interface{}, len(v))
	for i, val := range v {
		t[i] = val
	}

	err = ci.bulkcopy.AddRow(t)
	if err != nil {
		return
	}

	return driver.RowsAffected(0), nil
}

func (ci *copyin) Close() (err error) {
	return nil
}
