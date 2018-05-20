// +build go1.9

package mssql

import (
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"time"

	// "github.com/cockroachdb/apd"
	"cloud.google.com/go/civil"
)

// Type alias provided for compibility.

type MssqlDriver = Driver           // Deprecated: users should transition to the new name when possible.
type MssqlBulk = Bulk               // Deprecated: users should transition to the new name when possible.
type MssqlBulkOptions = BulkOptions // Deprecated: users should transition to the new name when possible.
type MssqlConn = Conn               // Deprecated: users should transition to the new name when possible.
type MssqlResult = Result           // Deprecated: users should transition to the new name when possible.
type MssqlRows = Rows               // Deprecated: users should transition to the new name when possible.
type MssqlStmt = Stmt               // Deprecated: users should transition to the new name when possible.

var _ driver.NamedValueChecker = &Conn{}

// VarChar parameter types.
type VarChar string

// DateTime1 encodes parameters to original DateTime SQL types.
type DateTime1 time.Time

// DateTimeOffset encodes parameters to DateTimeOffset, preserving the UTC offset.
type DateTimeOffset time.Time

func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch v := nv.Value.(type) {
	case sql.Out:
		if c.outs == nil {
			c.outs = make(map[string]interface{})
		}
		c.outs[nv.Name] = v.Dest

		// Unwrap the Out value and check the inner value.
		lnv := *nv
		lnv.Value = v.Dest
		err := c.CheckNamedValue(&lnv)
		if err != nil {
			if err != driver.ErrSkip {
				return err
			}
			lnv.Value, err = driver.DefaultParameterConverter.ConvertValue(lnv.Value)
			if err != nil {
				return err
			}
		}
		nv.Value = sql.Out{Dest: lnv.Value}
		return nil
	case VarChar:
		return nil
	case DateTime1:
		return nil
	case DateTimeOffset:
		return nil
	case civil.Date:
		return nil
	case civil.DateTime:
		return nil
	case civil.Time:
		return nil
	// case *apd.Decimal:
	// 	return nil
	default:
		return driver.ErrSkip
	}
}

func (s *Stmt) makeParamExtra(val driver.Value) (res param, err error) {
	switch val := val.(type) {
	case VarChar:
		res.ti.TypeId = typeBigVarChar
		res.buffer = []byte(val)
		res.ti.Size = len(res.buffer)
	case DateTime1:
		t := time.Time(val)
		res.ti.TypeId = typeDateTimeN
		res.ti.Size = 8
		res.buffer = make([]byte, 8)
		ref := time.Date(1900, 1, 1, 0, 0, 0, 0, t.Location())
		dur := t.Sub(ref)
		days := dur / (24 * time.Hour)
		tm := (300 * (dur % (24 * time.Hour))) / time.Second
		binary.LittleEndian.PutUint32(res.buffer[0:4], uint32(days))
		binary.LittleEndian.PutUint32(res.buffer[4:8], uint32(tm))
	case DateTimeOffset:
		t := time.Time(val)
		res.ti.TypeId = typeDateTimeOffsetN
		res.ti.Scale = 7
		res.ti.Size = 10
		buf := make([]byte, 10)
		res.buffer = buf
		days, ns := dateTime2(t)
		ns /= 100
		buf[0] = byte(ns)
		buf[1] = byte(ns >> 8)
		buf[2] = byte(ns >> 16)
		buf[3] = byte(ns >> 24)
		buf[4] = byte(ns >> 32)
		buf[5] = byte(days)
		buf[6] = byte(days >> 8)
		buf[7] = byte(days >> 16)
		_, offset := t.Zone()
		offset /= 60
		buf[8] = byte(offset)
		buf[9] = byte(offset >> 8)
	case civil.Date:
		res.ti.TypeId = typeDateN
		res.ti.Size = 3
		res.buffer = make([]byte, 3)
		buf := res.buffer
		days := val.DaysSince(civil.Date{Year: 1970, Month: time.January, Day: 1})
		// number of days since Jan 1 1 UTC
		days = days + 1969*365 + 1969/4 - 1969/100 + 1969/400
		buf[0] = byte(days)
		buf[1] = byte(days >> 8)
		buf[2] = byte(days >> 16)
	case civil.DateTime:
		res.ti.TypeId = typeDateTime2N
		res.ti.Scale = 7
		res.ti.Size = 8
		res.buffer = make([]byte, 8)
		buf := res.buffer

		dur := time.Hour*time.Duration(val.Time.Hour) +
			time.Minute*time.Duration(val.Time.Minute) +
			time.Second*time.Duration(val.Time.Second) +
			time.Duration(val.Time.Nanosecond)

		ns := int64(dur)
		ns /= 100
		buf[0] = byte(ns)
		buf[1] = byte(ns >> 8)
		buf[2] = byte(ns >> 16)
		buf[3] = byte(ns >> 24)
		buf[4] = byte(ns >> 32)

		days := val.Date.DaysSince(civil.Date{Year: 1970, Month: time.January, Day: 1})
		// number of days since Jan 1 1 UTC
		days = days + 1969*365 + 1969/4 - 1969/100 + 1969/400
		buf[5] = byte(days)
		buf[6] = byte(days >> 8)
		buf[7] = byte(days >> 16)
	case civil.Time:
		res.ti.TypeId = typeTimeN
		res.ti.Scale = 7
		res.ti.Size = 5
		res.buffer = make([]byte, 5)
		buf := res.buffer
		dur := time.Hour*time.Duration(val.Hour) +
			time.Minute*time.Duration(val.Minute) +
			time.Second*time.Duration(val.Second) +
			time.Duration(val.Nanosecond)

		ns := int64(dur)
		ns /= 100
		buf[0] = byte(ns)
		buf[1] = byte(ns >> 8)
		buf[2] = byte(ns >> 16)
		buf[3] = byte(ns >> 24)
		buf[4] = byte(ns >> 32)
	case sql.Out:
		res, err = s.makeParam(val.Dest)
		res.Flags = fByRevValue
	default:
		err = fmt.Errorf("mssql: unknown type for %T", val)
	}
	return
}
