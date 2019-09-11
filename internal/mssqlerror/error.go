package mssqlerror

import (
	"fmt"
)

// Error represents an SQL Server error. This
// type includes methods for reading the contents
// of the struct, which allows calling programs
// to check for specific error conditions without
// having to import this package directly.
type Error struct {
	Number     int32
	State      uint8
	Class      uint8
	Message    string
	ServerName string
	ProcName   string
	LineNo     int32
}

// Error returns the SQL Server error message.
func (e Error) Error() string {
	return "mssql: " + e.Message
}

// SQLErrorNumber returns the SQL Server error number.
func (e Error) SQLErrorNumber() int32 {
	return e.Number
}

// SQLErrorState returns the SQL Server error state.
func (e Error) SQLErrorState() uint8 {
	return e.State
}

// SQLErrorClass returns the SQL Server error class.
func (e Error) SQLErrorClass() uint8 {
	return e.Class
}

// SQLErrorMessage returns the SQL Server error message.
func (e Error) SQLErrorMessage() string {
	return e.Message
}

// SQLErrorServerName returns the SQL Server name.
func (e Error) SQLErrorServerName() string {
	return e.ServerName
}

// SQLErrorProcName returns the procedure name.
func (e Error) SQLErrorProcName() string {
	return e.ProcName
}

// SQLErrorLineNo returns the error line number.
func (e Error) SQLErrorLineNo() int32 {
	return e.LineNo
}

// StreamError represents TDS stream error.
type StreamError struct {
	Message string
}

// Error returns the TDS stream error message.
func (e StreamError) Error() string {
	return e.Message
}

func streamErrorf(format string, v ...interface{}) StreamError {
	return StreamError{"Invalid TDS stream: " + fmt.Sprintf(format, v...)}
}

// BadStreamPanic calls panic with err.
func BadStreamPanic(err error) {
	panic(err)
}

// BadStreamPanicf calls panic with a formatted error message as an invalid TDS stream error.
func BadStreamPanicf(format string, v ...interface{}) {
	panic(streamErrorf(format, v...))
}
