// Package typecivil allows "cloud.google.com/go/civil" parameters to be registered
// with the mssql connector.
package typecivil

import (
	"database/sql/driver"
	"time"

	"github.com/denisenkom/go-mssqldb/internal/mstype"

	"cloud.google.com/go/civil"
)

// Civil supports data types from "cloud.google.com/go/civil" for input parameters.
// These can't be directly scanned in yet until go1.11.
// Register with mssql.Connector.RegisterExtendedType(typecivil.Civil).
var Civil mstype.ExtendedTyper = ct{}

type ct struct{}

func (ct) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	default:
		return driver.ErrSkip
	case civil.Date:
		return nil
	case civil.DateTime:
		return nil
	case civil.Time:
		return nil
	}
}

func (ct) MakeParam(val driver.Value, p mstype.Param) error {
	switch val := val.(type) {
	default:
		return driver.ErrSkip
	case civil.Date:
		buf := p.SetValue(mstype.DateN, 0, 3)
		days := val.DaysSince(civil.Date{Year: 1970, Month: time.January, Day: 1})
		// number of days since Jan 1 1 UTC
		days = days + 1969*365 + 1969/4 - 1969/100 + 1969/400
		buf[0] = byte(days)
		buf[1] = byte(days >> 8)
		buf[2] = byte(days >> 16)
		return nil
	case civil.DateTime:
		buf := p.SetValue(mstype.DateTime2N, 7, 8)
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
		return nil
	case civil.Time:
		buf := p.SetValue(mstype.TimeN, 7, 5)
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
		return nil
	}
}
