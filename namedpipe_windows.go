// +build windows

package mssql

import (
	"fmt"
	"gopkg.in/natefinch/npipe.v2"
	"net"
)

func dialConnectionUsingNamedPipe(p connectParams) (conn net.Conn, err error) {
	conn, err = npipe.Dial(p.host)

	if err != nil {
		f := "Unable to open named pipe connection with address '%v': %v"
		return nil, fmt.Errorf(f, p.host, err.Error())
	}

	return conn, err
}
