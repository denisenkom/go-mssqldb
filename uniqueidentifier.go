package mssql

import (
	"bytes"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
)

var nilUUID = make([]byte, 16) // RFC 4122 section 4.1.7 says a nil UUID is all zeros.

type UniqueIdentifier []byte

func (u *UniqueIdentifier) Scan(v interface{}) error {
	reverse := func(b []byte) {
		for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
			b[i], b[j] = b[j], b[i]
		}
	}

	switch vt := v.(type) {
	case []byte:
		if len(vt) != 16 {
			return errors.New("mssql: invalid UniqueIdentifier length")
		}

		raw := make(UniqueIdentifier, 16)

		copy(raw, vt)

		reverse(raw[0:4])
		reverse(raw[4:6])
		reverse(raw[6:8])
		*u = raw

		return nil
	case string:
		if len(vt) != 36 {
			return errors.New("mssql: invalid UniqueIdentifier string length")
		}

		b := []byte(vt)
		for i, c := range b {
			switch c {
			case '-':
				b = append(b[:i], b[i+1:]...)
			}
		}

		*u = make(UniqueIdentifier, 16)
		_, err := hex.Decode(*u, []byte(b))
		return err
	default:
		return fmt.Errorf("mssql: cannot convert %T to UniqueIdentifier", v)
	}
}

func (u UniqueIdentifier) Value() (driver.Value, error) {
	reverse := func(b []byte) {
		for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
			b[i], b[j] = b[j], b[i]
		}
	}

	if len([]byte(u)) != 16 {
		return nil, errors.New("mssql: invalid UniqueIdentifier length")
	}

	raw := make([]byte, 16)

	copy(raw, u)

	reverse(raw[0:4])
	reverse(raw[4:6])
	reverse(raw[6:8])

	return raw, nil
}

func (u UniqueIdentifier) String() string {
	b := []byte(u)
	if len(b) != 16 {
		b = nilUUID
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (u UniqueIdentifier) Equal(u2 UniqueIdentifier) bool {
	return bytes.Equal(u, u2)
}
