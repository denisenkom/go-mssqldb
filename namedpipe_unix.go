// +build !windows

package mssql

import (
	"fmt"
	"net"
)

func dialConnectionUsingNamedPipe(p connectParams) (conn net.Conn, err error) {
	return nil, fmt.Errorf(
		"Named pipe protocol (\"%s\") is implemented only for Windows OS",
		p.protocol)
}
