// +build go1.10

package mssql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestNewAccessTokenConnector(t *testing.T) {
	dsn := "Server=server.database.windows.net;Database=db"
	tp := func() (string, error) { return "token", nil }
	type args struct {
		dsn           string
		tokenProvider func() (string, error)
	}
	tests := []struct {
		name    string
		args    args
		want    func(driver.Connector) error
		wantErr bool
	}{
		{
			name: "Happy path",
			args: args{
				dsn:           dsn,
				tokenProvider: tp},
			want: func(c driver.Connector) error {
				tc, ok := c.(*Connector)
				if !ok {
					return fmt.Errorf("Expected driver to be of type *Connector, but got %T", c)
				}
				p := tc.params
				if p.Database != "db" {
					return fmt.Errorf("expected params.database=db, but got %v", p.Database)
				}
				if p.Host != "server.database.windows.net" {
					return fmt.Errorf("expected params.host=server.database.windows.net, but got %v", p.Host)
				}
				if tc.securityTokenProvider == nil {
					return fmt.Errorf("Expected federated authentication provider to not be nil")
				}
				t, err := tc.securityTokenProvider(context.TODO())
				if t != "token" || err != nil {
					return fmt.Errorf("Unexpected results from tokenProvider: %v, %v", t, err)
				}
				return nil
			},
			wantErr: false,
		},
		{
			name: "Nil tokenProvider gives error",
			args: args{
				dsn:           dsn,
				tokenProvider: nil},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewAccessTokenConnector(tt.args.dsn, tt.args.tokenProvider)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewAccessTokenConnector() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.want != nil {
				if err := tt.want(got); err != nil {
					t.Error(err)
				}
			}
		})
	}
}

func TestAccessTokenConnectorFailsToConnectIfNoAccessToken(t *testing.T) {
	errorText := "This is a test"
	dsn := "Server=server.database.windows.net;Database=db"
	tp := func() (string, error) { return "", errors.New(errorText) }
	sut, err := NewAccessTokenConnector(dsn, tp)
	if err != nil {
		t.Fatalf("expected err==nil, but got %+v", err)
	}
	_, err = sut.Connect(context.TODO())
	if err == nil || !strings.Contains(err.Error(), errorText) {
		t.Fatalf("expected error to contain %q, but got %q", errorText, err)
	}
}
