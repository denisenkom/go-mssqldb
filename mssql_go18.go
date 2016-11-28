// +build go1.8

package mssql

import (
	"database/sql/driver"
	"context"
	"database/sql"
	"errors"
)

func (c *MssqlConn) BeginContext(ctx context.Context) (driver.Tx, error) {
	tdsIsolation := isolationUseCurrent
	isolation, ok := driver.IsolationFromContext(ctx)
	if ok {
		switch sql.IsolationLevel(isolation) {
		case sql.LevelDefault:
			tdsIsolation = isolationUseCurrent
		case sql.LevelReadUncommitted:
			tdsIsolation = isolationReadUncommited
		case sql.LevelReadCommitted:
			tdsIsolation = isolationReadCommited
		case sql.LevelWriteCommitted:
			return nil, errors.New("LevelWriteCommitted isolation level is not supported")
		case sql.LevelRepeatableRead:
			tdsIsolation = isolationRepeatableRead
		case sql.LevelSnapshot:
			tdsIsolation = isolationSnapshot
		case sql.LevelSerializable:
			tdsIsolation = isolationSerializable
		case sql.LevelLinearizable:
			return nil, errors.New("LevelLinearizable isolation level is not supported")
		default:
			return nil, errors.New("Isolation level is not supported or unknown")
		}
	}
	return c.begin(tdsIsolation)
}

func (c *MssqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.prepareContext(ctx, query)
}

func (s *MssqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	list := make([]namedValue, len(args))
	for i, nv := range args {
		list[i] = namedValue(nv)
	}
	return s.queryContext(ctx, list)
}

func (s *MssqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	list := make([]namedValue, len(args))
	for i, nv := range args {
		list[i] = namedValue(nv)
	}
	return s.exec(ctx, list)
}