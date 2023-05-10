package mssql

import (
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
)

type NullUniqueIdentifier struct {
	Valid bool
	UUID  [16]byte
}

func (u *NullUniqueIdentifier) Scan(v interface{}) error {
	if v == nil {
		u.UUID = [16]byte{}
		u.Valid = false
		return nil
	}

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

		var raw [16]byte

		copy(raw[:], vt)

		reverse(raw[0:4])
		reverse(raw[4:6])
		reverse(raw[6:8])
		u.UUID = raw
		u.Valid = true

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

		_, err := hex.Decode(u.UUID[:], []byte(b))
		if err == nil {
			u.Valid = true
		}
		return err
	default:
		return fmt.Errorf("mssql: cannot convert %T to UniqueIdentifier", v)
	}
}

func (u NullUniqueIdentifier) Value() (driver.Value, error) {
	if u.Valid == false {
		return nil, nil
	}

	reverse := func(b []byte) {
		for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
			b[i], b[j] = b[j], b[i]
		}
	}

	raw := make([]byte, len(u.UUID))
	copy(raw, u.UUID[:])

	reverse(raw[0:4])
	reverse(raw[4:6])
	reverse(raw[6:8])

	return raw, nil
}

func (u NullUniqueIdentifier) String() string {
	if u.Valid == false {
		return ""
	}

	return fmt.Sprintf("%X-%X-%X-%X-%X", u.UUID[0:4], u.UUID[4:6], u.UUID[6:8], u.UUID[8:10], u.UUID[10:])
}

// MarshalText converts Uniqueidentifier to bytes corresponding to the stringified hexadecimal representation of the Uniqueidentifier
// e.g., "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA" -> [65 65 65 65 65 65 65 65 45 65 65 65 65 45 65 65 65 65 45 65 65 65 65 65 65 65 65 65 65 65 65]
func (u NullUniqueIdentifier) MarshalText() []byte {
	return []byte(u.String())
}
