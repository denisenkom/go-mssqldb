// +build go1.14

package mssql

import (
	"testing"
)

func BenchmarkMessageQueue(b *testing.B) {
	conn, logger := open(b)
	defer conn.Close()
	defer logger.StopLogging()

	b.Run("BlockingQuery", func(b *testing.B) {
		var errs, results float64
		for i := 0; i < b.N; i++ {
			r, err := conn.Query(mixedQuery)
			if err != nil {
				b.Fatal(err.Error())
			}
			defer r.Close()
			active := true
			first := true
			for active {
				active = r.Next()
				if active && first {
					results++
				}
				first = false
				if !active {
					if r.Err() != nil {
						b.Logf("r.Err:%v", r.Err())
						errs++
					}
					active = r.NextResultSet()
					if active {
						first = true
					}
				}
			}
		}
		b.ReportMetric(float64(0), "msgs/op")
		b.ReportMetric(errs/float64(b.N), "errors/op")
		b.ReportMetric(results/float64(b.N), "results/op")
	})
	b.Run("NonblockingQuery", func(b *testing.B) {
		var msgs, errs, results, rowcounts float64
		for i := 0; i < b.N; i++ {
			m, e, r, rc := testMixedQuery(conn, b)
			msgs += float64(m)
			errs += float64(e)
			results += float64(r)
			rowcounts += float64(rc)
			if r != 4 {
				b.Fatalf("Got wrong results count: %d, expected 4", r)
			}
		}
		b.ReportMetric(msgs/float64(b.N), "msgs/op")
		b.ReportMetric(errs/float64(b.N), "errors/op")
		b.ReportMetric(results/float64(b.N), "results/op")
		b.ReportMetric(rowcounts/float64(b.N), "rowcounts/op")
	})
}
