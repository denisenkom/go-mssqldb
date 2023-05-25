package msdsn

import (
	"fmt"
	"strings"
	"testing"
)

type testProtocol struct{}

var protocolImpl = testProtocol{}

func (t testProtocol) Hidden() bool {
	return false
}

func (t testProtocol) ParseServer(server string, p *Config) error {
	if strings.HasPrefix(server, "**") {
		p.ProtocolParameters[t.Protocol()] = "special"
	}
	if server == "fail" {
		return fmt.Errorf("ParseServer fail")
	}
	// p.Host is empty if tst protocol was specified
	if p.Host == "" {
		p.Host = strings.TrimPrefix(server, "**")
	}
	return nil
}

func (t testProtocol) Protocol() string {
	return "tst"
}

func init() {
	ProtocolParsers = append(ProtocolParsers, protocolImpl)
}

func TestProtocolParseExtension(t *testing.T) {
	type tst struct {
		dsn            string
		expectedConfig func(c *Config) bool
	}
	tests := []tst{
		{"server=myserver", func(c *Config) bool {
			return len(c.Protocols) == 2 && c.Protocols[0] == "tcp" && c.Protocols[1] == "tst" && c.Host == "myserver" && c.ProtocolParameters["tst"] == nil
		}},
		{"server=**myserver", func(c *Config) bool {
			return len(c.Protocols) == 2 && c.Protocols[0] == "tcp" && c.Protocols[1] == "tst" && c.Host == "**myserver" && c.ProtocolParameters["tst"] == "special"
		}},
		{"server=tst:**myserver", func(c *Config) bool {
			return len(c.Protocols) == 1 && c.Protocols[0] == "tst" && c.Host == "myserver" && c.ProtocolParameters["tst"] == "special"
		}},
		{"server=tst:myserver", func(c *Config) bool {
			return len(c.Protocols) == 1 && c.Protocols[0] == "tst" && c.Host == "myserver" && c.ProtocolParameters["tst"] == nil
		}},
		{"sqlserver://user@myserver", func(c *Config) bool {
			return len(c.Protocols) == 2 && c.Protocols[0] == "tcp" && c.Protocols[1] == "tst" && c.Host == "myserver" && c.ProtocolParameters["tst"] == nil
		}},
		{"sqlserver://**myserver", func(c *Config) bool {
			return len(c.Protocols) == 2 && c.Protocols[0] == "tcp" && c.Protocols[1] == "tst" && c.Host == "**myserver" && c.ProtocolParameters["tst"] == "special"
		}},
		{"sqlserver://**myserver?protocol=tst", func(c *Config) bool {
			return len(c.Protocols) == 1 && c.Protocols[0] == "tst" && c.Host == "myserver" && c.ProtocolParameters["tst"] == "special"
		}},
		{"sqlserver://myserver?protocol=tst", func(c *Config) bool {
			return len(c.Protocols) == 1 && c.Protocols[0] == "tst" && c.Host == "myserver" && c.ProtocolParameters["tst"] == nil
		}},
		{"sqlserver://fail", func(c *Config) bool {
			return len(c.Protocols) == 1 && c.Protocols[0] == "tcp" && c.Host == "fail" && c.ProtocolParameters["tst"] == nil
		}},
	}
	for _, test := range tests {
		c, err := Parse(test.dsn)
		if err != nil {
			t.Fatalf("Unexpected error parsing '%s':'%s'", test.dsn, err.Error())
		}
		if !test.expectedConfig(&c) {
			t.Fatalf("Config validation failed for '%s'. Config: '%v'", test.dsn, c)
		}
	}
}
