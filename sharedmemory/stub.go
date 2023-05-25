//go:build !windows
// +build !windows

package sharedmemory

import (
	"context"
	"fmt"
	"net"

	"github.com/microsoft/go-mssqldb/msdsn"
)

func (n sharedMemoryDialer) ParseServer(server string, p *msdsn.Config) error {
	return fmt.Errorf("Shared memory connections are not supported on this operating system")
}

func (n sharedMemoryDialer) Protocol() string {
	return "np"
}

func (n sharedMemoryDialer) Hidden() bool {
	return false
}

func (n sharedMemoryDialer) ParseBrowserData(data msdsn.BrowserData, p *msdsn.Config) error {
	return fmt.Errorf("Shared memory connections are not supported on this operating system")
}

func (n sharedMemoryDialer) DialConnection(ctx context.Context, p *msdsn.Config) (conn net.Conn, err error) {

	return nil, fmt.Errorf("Shared memory connections are not supported on this operating system")
}

func (n sharedMemoryDialer) CallBrowser(p *msdsn.Config) bool {
	return false
}
