package mssql

import (
    "fmt"
)

type Error struct {
    Number int32
    State uint8
    Class uint8
    Message string
    ServerName string
    ProcName string
    LineNo int32
    timeout bool
}

func (e Error) Timeout() bool {
    return e.timeout
}


func (e Error) Error() string {
    return "mssql: " + e.Message
}


type StreamError struct {
    Message string
}

func (e StreamError) Error() string {
    return e.Message
}


func streamErrorf(format string, v ...interface{}) StreamError {
    return StreamError{"Invalid TDS stream: " + fmt.Sprintf(format, v...)}
}
