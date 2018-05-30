package testutil

import (
	"net/url"
	"os"
	"testing"
)

// MakeConnStr returns a URL struct so it may be modified by various
// tests before used as a DSN.
func MakeConnStr(t *testing.T) *url.URL {
	dsn := os.Getenv("SQLSERVER_DSN")
	if len(dsn) > 0 {
		parsed, err := url.Parse(dsn)
		if err != nil {
			t.Fatal("unable to parse SQLSERVER_DSN as URL", err)
		}
		values := parsed.Query()
		if values.Get("log") == "" {
			values.Set("log", "127")
		}
		parsed.RawQuery = values.Encode()
		return parsed
	}
	values := url.Values{}
	values.Set("log", "127")
	values.Set("database", os.Getenv("DATABASE"))
	return &url.URL{
		Scheme:   "sqlserver",
		Host:     os.Getenv("HOST"),
		Path:     os.Getenv("INSTANCE"),
		User:     url.UserPassword(os.Getenv("SQLUSER"), os.Getenv("SQLPASSWORD")),
		RawQuery: values.Encode(),
	}
}
