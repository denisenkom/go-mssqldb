package mssql

import (
	"fmt"
	"testing"
	"time"
)

func TestDateTimeParam19(t *testing.T) {
	conn, logger := open(t)
	defer conn.Close()
	logger.StopLogging()

	// testing DateTime1, only supported on go 1.9
	var emptydate time.Time
	mindate1 := time.Date(1753, 1, 1, 0, 0, 0, 0, time.UTC)
	maxdate1 := time.Date(9999, 12, 31, 23, 59, 59, 997000000, time.UTC)
	testdates1 := []DateTime1{
		DateTime1(mindate1),
		DateTime1(maxdate1),
		DateTime1(time.Date(1752, 12, 31, 23, 59, 59, 997000000, time.UTC)), // just a little below minimum date
		DateTime1(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)),             // just a little over maximum date
		DateTime1(emptydate),
	}

	for _, test := range testdates1 {
		t.Run(fmt.Sprintf("Test datetime for %v", test), func(t *testing.T) {
			var res time.Time
			expected := time.Time(test)
			queryParamRoundTrip(conn, test, &res)
			// clip value
			if expected.Before(mindate1) {
				expected = mindate1
			}
			if expected.After(maxdate1) {
				expected = maxdate1
			}
			if expected.Sub(res) != 0 {
				t.Errorf("expected: '%s', got: '%s' delta: %d", expected, res, expected.Sub(res))
			}
		})
	}
}
