package np

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/microsoft/go-mssqldb/internal/gopkg.in/natefinch/npipe.v2"
)

func DialConnection(ctx context.Context, pipename string, host string, instanceName string, inputServerSPN string) (conn net.Conn, serverSPN string, err error) {
	dl, ok := ctx.Deadline()
	if ok {
		duration := time.Until(dl)
		conn, err = npipe.DialTimeoutExisting(pipename, duration)
	} else {
		conn, err = npipe.DialExisting(pipename)
	}
	serverSPN = inputServerSPN
	if err == nil && inputServerSPN == "" {
		instance := ""
		if instanceName != "" {
			instance = fmt.Sprintf(":%s", instanceName)
		}
		ip := net.ParseIP(host)
		if ip != nil && ip.IsLoopback() {
			host, _ = os.Hostname()
		}
		serverSPN = fmt.Sprintf("MSSQLSvc/%s%s", host, instance)
	}
	return
}
