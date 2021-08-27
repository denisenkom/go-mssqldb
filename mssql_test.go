package mssql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"

	"github.com/denisenkom/go-mssqldb/msdsn"
)

func TestBadOpen(t *testing.T) {
	tl := testLogger{t: t}
	defer tl.StopLogging()
	drv := driverWithProcess(t, &tl)
	_, err := drv.open(context.Background(), "port=bad")
	if err == nil {
		t.Fail()
	}
}

func TestIsProc(t *testing.T) {
	list := []struct {
		s  string
		is bool
	}{
		{"proc", true},
		{"select 1;", false},
		{"select 1", false},
		{"[proc 1]", true},
		{"[proc\n1]", true},
		{"schema.name", true},
		{"[schema].[name]", true},
		{"schema.[name]", true},
		{"[schema].name", true},
		{"schema.[proc name]", true},
		{"db.schema.[proc name]", true},
		{"db..[proc name]", true},
		{"#temp_@_proc", true},
		{"_temp.[_proc]", true},
		{"raiserror(13000,1,1)", false},
		{"select*from(@table)", false},
		{"select[A]]]+1from[B]", false},
		{"--proc", false},
		{"[proc;]", true},
		{" proc", false},
	}

	for _, item := range list {
		got := isProc(item.s)
		if got != item.is {
			t.Errorf("for %q, got %t want %t", item.s, got, item.is)
		}
	}
}

func TestConvertIsolationLevel(t *testing.T) {
	level, err := convertIsolationLevel(sql.LevelReadUncommitted)
	if level != isolationReadUncommited || err != nil {
		t.Fatal("invalid value returned")
	}
	level, err = convertIsolationLevel(sql.LevelReadCommitted)
	if level != isolationReadCommited || err != nil {
		t.Fatal("invalid value returned")
	}
	level, err = convertIsolationLevel(sql.LevelRepeatableRead)
	if level != isolationRepeatableRead || err != nil {
		t.Fatal("invalid value returned")
	}
	level, err = convertIsolationLevel(sql.LevelSnapshot)
	if level != isolationSnapshot || err != nil {
		t.Fatal("invalid value returned")
	}
	_, err = convertIsolationLevel(sql.LevelWriteCommitted)
	if err == nil {
		t.Fatal("must fail but it didn't")
	}
	_, err = convertIsolationLevel(sql.LevelLinearizable)
	if err == nil {
		t.Fatal("must fail but it didn't")
	}
	_, err = convertIsolationLevel(sql.IsolationLevel(1000))
	if err == nil {
		t.Fatal("must fail but it didn't")
	}
}

// equalErrors is a helper function that compares two errors
// by comparing their nilness, underlying type, and Error messages
func equalErrors(e1 error, e2 error) bool {
	if e1 == nil && e2 == nil {
		return true
	}
	if e1 == nil && e2 != nil || e1 != nil && e2 == nil {
		return false
	}
	return reflect.TypeOf(e1) == reflect.TypeOf(e2) &&
		e1.Error() == e2.Error()
}

// TestCheckBadConn verifies that different combinations of
// configuration, inputs and errors result in the proper output
// error and connection state.
func TestCheckBadConn(t *testing.T) {

	netErr := &net.OpError{Err: fmt.Errorf("fake net.Error")}
	streamErr := StreamError{Message: "fake StreamError"}
	serverErr := ServerError{sqlError: Error{Message: "fake ServerError"}}
	goodConnErr := fmt.Errorf("fake error that leaves connection good")

	testInputs := []struct {
		err              error
		mayRetry         bool
		disableRetry     bool
		expectedErr      error
		expectedConnGood bool
	}{
		{nil, false, false, nil, true},
		{nil, true, false, nil, true},
		{nil, false, true, nil, true},
		{nil, true, true, nil, true},
		{io.EOF, false, false, io.EOF, false},
		{io.EOF, true, false, newRetryableError(io.EOF), false},
		{io.EOF, false, true, io.EOF, false},
		{io.EOF, true, true, io.EOF, false},
		{netErr, false, false, netErr, false},
		{netErr, true, false, newRetryableError(netErr), false},
		{netErr, false, true, netErr, false},
		{netErr, true, true, netErr, false},
		{streamErr, false, false, streamErr, false},
		{streamErr, true, false, newRetryableError(streamErr), false},
		{streamErr, false, true, streamErr, false},
		{streamErr, true, true, streamErr, false},
		{serverErr, false, false, serverErr, false},
		{serverErr, true, false, newRetryableError(serverErr), false},
		{serverErr, false, true, serverErr, false},
		{serverErr, true, true, serverErr, false},
		{goodConnErr, false, false, goodConnErr, true},
		{goodConnErr, true, false, goodConnErr, true},
		{goodConnErr, false, true, goodConnErr, true},
		{goodConnErr, true, true, goodConnErr, true},
	}

	c := Conn{
		connector: &Connector{
			params: msdsn.Config{},
		},
		sess: &tdsSession{
			logger: optionalLogger{},
		},
	}

	for _, ti := range testInputs {
		c.connectionGood = true
		c.connector.params.DisableRetry = ti.disableRetry
		actualErr := c.checkBadConn(context.Background(), ti.err, ti.mayRetry)
		if !equalErrors(actualErr, ti.expectedErr) ||
			c.connectionGood != ti.expectedConnGood {
			t.Fatalf("checkBadConn returned unexpected result for input err = '%+v', mayRetry = '%t', disableRetry = '%t': "+
				"got output err = '%+v', connectionGood = '%t', "+
				"wanted output err = '%+v', connectionGood = '%t'",
				ti.err, ti.mayRetry, ti.disableRetry, actualErr, c.connectionGood, ti.expectedErr, ti.expectedConnGood)
		}
	}

	// This must be the final test in this function, because we expect it to panic
	defer func() { recover() }()
	c.checkBadConn(context.Background(), driver.ErrBadConn, true)
	t.Fatalf("checkBadConn did not panic as expected when passed driverErrBadConn")
}

// TestBadConnRejection verifies that database operations that start
// with a bad connection fail with the sentinel error driver.ErrBadConn.
// That instructs the database/sql connection pool logic to discard the
// bad connection and, if appropriate, attempt to retry the operation
// with another connection.
func TestBadConnRejection(t *testing.T) {

	c := Conn{connectionGood: false}

	if _, err := c.Begin(); err != driver.ErrBadConn {
		t.Fatalf("Begin did not fail on bad connection: "+
			"got err = '%+v', wanted err = '%+v'", err, driver.ErrBadConn)
	}

	if err := c.Commit(); err != driver.ErrBadConn {
		t.Fatalf("Commit did not fail on bad connection: "+
			"got err = '%+v', wanted err = '%+v'", err, driver.ErrBadConn)
	}

	if err := c.Rollback(); err != driver.ErrBadConn {
		t.Fatalf("Rollback did not fail on bad connection: "+
			"got err = '%+v', wanted err = '%+v'", err, driver.ErrBadConn)
	}

	if _, err := c.Prepare("query"); err != driver.ErrBadConn {
		t.Fatalf("Prepare did not fail on bad connection: "+
			"got err = '%+v', wanted err = '%+v'", err, driver.ErrBadConn)
	}

	if _, err := c.PrepareContext(context.Background(), "query"); err != driver.ErrBadConn {
		t.Fatalf("PrepareContext did not fail on bad connection: "+
			"got err = '%+v', wanted err = '%+v'", err, driver.ErrBadConn)
	}

	if err := c.Ping(context.Background()); err != driver.ErrBadConn {
		t.Fatalf("Ping did not fail on bad connection: "+
			"got err = '%+v', wanted err = '%+v'", err, driver.ErrBadConn)
	}

	if _, err := c.BeginTx(context.Background(), driver.TxOptions{}); err != driver.ErrBadConn {
		t.Fatalf("BeginTx did not fail on bad connection: "+
			"got err = '%+v', wanted err = '%+v'", err, driver.ErrBadConn)
	}

	s := &Stmt{c: &c}

	if _, err := s.Query(nil); err != driver.ErrBadConn {
		t.Fatalf("Query did not fail on bad connection: "+
			"got err = '%+v', wanted err = '%+v'", err, driver.ErrBadConn)
	}

	if _, err := s.QueryContext(context.Background(), nil); err != driver.ErrBadConn {
		t.Fatalf("QueryContext did not fail on bad connection: "+
			"got err = '%+v', wanted err = '%+v'", err, driver.ErrBadConn)
	}

	if _, err := s.Exec(nil); err != driver.ErrBadConn {
		t.Fatalf("Exec did not fail on bad connection: "+
			"got err = '%+v', wanted err = '%+v'", err, driver.ErrBadConn)
	}

	if _, err := s.ExecContext(context.Background(), nil); err != driver.ErrBadConn {
		t.Fatalf("ExecContext did not fail on bad connection: "+
			"got err = '%+v', wanted err = '%+v'", err, driver.ErrBadConn)
	}

	r := &Rows{stmt: s}

	if err := r.Next(nil); err != driver.ErrBadConn {
		t.Fatalf("Next did not fail on bad connection: "+
			"got err = '%+v', wanted err = '%+v'", err, driver.ErrBadConn)
	}

}
