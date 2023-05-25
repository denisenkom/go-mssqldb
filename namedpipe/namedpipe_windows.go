package namedpipe

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strings"

	"github.com/microsoft/go-mssqldb/internal/np"
	"github.com/microsoft/go-mssqldb/msdsn"
)

type namedPipeData struct {
	PipeName string
}

var azureDomains = []string{
	".database.windows.net",
	".database.chinacloudapi.cn",
	".database.usgovcloudapi.net",
}

func (n namedPipeDialer) ParseServer(server string, p *msdsn.Config) error {
	if p.Port > 0 {
		return fmt.Errorf("Named pipes disallowed due to port being specified")
	}
	if strings.HasPrefix(server, `\\`) {
		// assume a server name starting with \\ is the full named pipe path
		p.ProtocolParameters[n.Protocol()] = namedPipeData{PipeName: server}
	} else if p.Host == "" { // if the string specifies np:host\instance, tcpParser won't have filled in p.Host
		parts := strings.SplitN(server, `\`, 2)
		p.Host = parts[0]
		if p.Host == "." || strings.ToUpper(p.Host) == "(LOCAL)" {
			p.Host = "localhost"
		}
		if len(parts) > 1 {
			p.Instance = parts[1]
		}
	} else {
		host := strings.ToLower(p.Host)
		for _, domain := range azureDomains {
			if strings.HasSuffix(host, domain) {
				return fmt.Errorf("Named pipes disallowed for Azure SQL Database connections")
			}
		}
	}
	pipe, ok := p.Parameters["pipe"]
	if ok {
		p.ProtocolParameters[n.Protocol()] = namedPipeData{PipeName: fmt.Sprintf(`\\%s\pipe\%s`, p.Host, pipe)}
	}
	return nil
}

func (n namedPipeDialer) Protocol() string {
	return "np"
}

func (n namedPipeDialer) Hidden() bool {
	return false
}

func (n namedPipeDialer) ParseBrowserData(data msdsn.BrowserData, p *msdsn.Config) error {
	// If instance is specified, but no port, check SQL Server Browser
	// for the instance and discover its port.
	p.Instance = strings.ToUpper(p.Instance)
	instance := p.Instance
	if instance == "" {
		instance = "MSSQLSERVER"
	}
	ok := len(data) > 0
	pipename := ""
	if ok {
		pipename, ok = data[instance]["np"]
	}
	if !ok {
		f := "no named pipe instance matching '%v' returned from host '%v'"
		return fmt.Errorf(f, p.Instance, p.Host)
	}
	p.ProtocolParameters[n.Protocol()] = namedPipeData{PipeName: pipename}
	return nil
}

func (n namedPipeDialer) DialConnection(ctx context.Context, p *msdsn.Config) (conn net.Conn, err error) {
	data := p.ProtocolParameters[n.Protocol()]
	switch d := data.(type) {
	case namedPipeData:
		serverSPN := p.ServerSPN
		conn, serverSPN, err = np.DialConnection(ctx, d.PipeName, p.Host, p.Instance, serverSPN)
		if err == nil && p.ServerSPN == "" {
			p.ServerSPN = serverSPN
		}
		return
	}
	return nil, fmt.Errorf("Unexpected protocol data specified for connection: %v", reflect.TypeOf(data))
}

func (n namedPipeDialer) CallBrowser(p *msdsn.Config) bool {
	_, ok := p.ProtocolParameters[n.Protocol()]
	return !ok
}
