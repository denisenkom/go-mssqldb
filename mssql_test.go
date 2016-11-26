package mssql

import (
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

type netError struct{}

func (e netError) Timeout() bool {
	return false
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
			t.Errorf("%s: unexpected error, got %v, expected %v", tt.name, actual, tt.expected)
		}
	}
}
