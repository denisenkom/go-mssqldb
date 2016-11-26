package mssql

import (
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

type netError struct{}

func (e netError) Timeout() bool {
	return true
}

func (e netError) Temporary() bool {
	return true
}

func (e netError) Error() string {
	return "dummy network error"
}

func TestCheckBadConn(t *testing.T) {
	err := errors.New("not a network error")
	tests := []struct {
		name     string
		err      error
		expected error
	}{
		{
			name:     "network error",
			err:      netError{},
			expected: driver.ErrBadConn,
		}, {
			name:     "EOF",
			err:      io.EOF,
			expected: driver.ErrBadConn,
		}, {
			name:     "not an I/O error",
			err:      err,
			expected: err,
		},
	}

	for _, tt := range tests {
		actual := CheckBadConn(tt.err)
		if actual != tt.expected {
			t.Error("%s: unexpected error.", tt.name)
		}
	}
}

func TestParseConnectionStringHappyCase(t *testing.T) {

	res := parseConnectionString("server=foo;user id=bar;password=baz;encrypt=true;TrustServerCertificate=true")
	if val, ok := res["server"]; ok {
		if val != "foo" {
			t.Errorf("parseConnectionString; result key 'server'; expected 'foo', got '%s'", val)
		}
	} else {
		t.Error("parseConnectionString; result key 'server'; key is missing")
	}
	if val, ok := res["user id"]; ok {
		if val != "bar" {
			t.Errorf("parseConnectionString; result key 'user id'; expected 'bar', got '%s'", val)
		}
	} else {
		t.Error("parseConnectionString; result key 'user id'; key is missing")
	}
	if val, ok := res["password"]; ok {
		if val != "baz" {
			t.Errorf("parseConnectionString; result key 'password'; expected 'baz', got '%s'", val)
		}
	} else {
		t.Error("parseConnectionString; result key 'password'; key is missing")
	}
	if val, ok := res["encrypt"]; ok {
		if val != "true" {
			t.Errorf("parseConnectionString; result key 'encrypt'; expected 'true', got '%s'", val)
		}
	} else {
		t.Error("parseConnectionString; result key 'encrypt'; key is missing")
	}
	if val, ok := res["trustservercertificate"]; ok {
		if val != "true" {
			t.Errorf("parseConnectionString; result key 'TrustServerCertificate'; expected 'true', got '%s'", val)
		}
	} else {
		t.Error("parseConnectionString; result key 'TrustServerCertificate'; key is missing")
	}
}

func TestParseConnectionStringPasswordHasSemicolon(t *testing.T) {

	res := parseConnectionString("server=foo\tuser id=bar\tpassword=baz;\tencrypt=true\tTrustServerCertificate=true")
	if val, ok := res["password"]; ok {
		if val != "baz;" {
			t.Errorf("parseConnectionString; result key 'password'; expected 'baz;', got '%s'", val)
		}
	} else {
		t.Error("parseConnectionString; result key 'password'; key is missing")
	}
}

func TestParseConnectionStringPasswordHasLeadingSpace(t *testing.T) {

	res := parseConnectionString("server=foo\tuser id=bar\tpassword= baz\tencrypt=true\tTrustServerCertificate=true")
	if val, ok := res["password"]; ok {
		if val != " baz" {
			t.Errorf("parseConnectionString; result key 'password'; expected ' baz', got '%s'", val)
		}
	} else {
		t.Error("parseConnectionString; result key 'password'; key is missing")
	}
}

func TestParseConnectionStringPasswordHasTrailingSpace(t *testing.T) {

	res := parseConnectionString("server=foo\tuser id=bar\tpassword=baz \tencrypt=true\tTrustServerCertificate=true")
	if val, ok := res["password"]; ok {
		if val != "baz " {
			t.Errorf("parseConnectionString; result key 'password'; expected 'baz ', got '%s'", val)
		}
	} else {
		t.Error("parseConnectionString; result key 'password'; key is missing")
	}
}

func TestParseConnectionStringUserIdHasSemicolon(t *testing.T) {

	res := parseConnectionString("server=foo\tuser id=bar;\tpassword=baz\tencrypt=true\tTrustServerCertificate=true")
	if val, ok := res["user id"]; ok {
		if val != "bar;" {
			t.Errorf("parseConnectionString; result key 'user id'; expected 'bar;', got '%s'", val)
		}
	} else {
		t.Error("parseConnectionString; result key 'user id'; key is missing")
	}
}

func TestParseConnectionStringUserIdHasLeadingSpace(t *testing.T) {

	res := parseConnectionString("server=foo\tuser id= bar\tpassword=baz\tencrypt=true\tTrustServerCertificate=true")
	if val, ok := res["user id"]; ok {
		if val != " bar" {
			t.Errorf("parseConnectionString; result key 'user id'; expected ' bar', got '%s'", val)
		}
	} else {
		t.Error("parseConnectionString; result key 'user id'; key is missing")
	}
}

func TestParseConnectionStringUserIdHasTrailingSpace(t *testing.T) {

	res := parseConnectionString("server=foo\tuser id=bar \tpassword=baz\tencrypt=true\tTrustServerCertificate=true")
	if val, ok := res["user id"]; ok {
		if val != "bar " {
			t.Errorf("parseConnectionString; result key 'user id'; expected 'bar ', got '%s'", val)
		}
	} else {
		t.Error("parseConnectionString; result key 'user id'; key is missing")
	}
}

func TestParseConnectionStringPasswordHasCamelCaseKeyAndTrailingSpace(t *testing.T) {

	res := parseConnectionString("server=foo\tuser id=bar\tPassWord=baz \tencrypt=true\tTrustServerCertificate=true")
	if val, ok := res["password"]; ok {
		if val != "baz " {
			t.Errorf("parseConnectionString; result key 'password'; expected 'baz ', got '%s'", val)
		}
	} else {
		t.Error("parseConnectionString; result key 'password'; key is missing")
	}
}

func TestParseConnectionStringUserIdHasCamelCaseKeyAndTrailingSpace(t *testing.T) {

	res := parseConnectionString("server=foo\tUser ID=bar \tpassword=baz\tencrypt=true\tTrustServerCertificate=true")
	if val, ok := res["user id"]; ok {
		if val != "bar " {
			t.Errorf("parseConnectionString; result key 'user id'; expected 'bar ', got '%s'", val)
		}
	} else {
		t.Error("parseConnectionString; result key 'user id'; key is missing")
	}
}

