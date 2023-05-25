//go:build !windows
// +build !windows

package namedpipe

import (
	"context"
	"fmt"
	"net"

	"github.com/microsoft/go-mssqldb/msdsn"
)

func (n namedPipeDialer) ParseServer(server string, p *msdsn.Config) error {
	return fmt.Errorf("Named pipe connections are not supported on this operating system")
}

func (n namedPipeDialer) Protocol() string {
	return "np"
}

func (n namedPipeDialer) Hidden() bool {
	return false
}

func (n namedPipeDialer) ParseBrowserData(data msdsn.BrowserData, p *msdsn.Config) error {
	return fmt.Errorf("Named pipe connections are not supported on this operating system")
}

func (n namedPipeDialer) DialConnection(ctx context.Context, p *msdsn.Config) (conn net.Conn, err error) {

	return nil, fmt.Errorf("Named pipe connections are not supported on this operating system")
}

func (n namedPipeDialer) CallBrowser(p *msdsn.Config) bool {
	return false
}
