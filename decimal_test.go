package mssql

import (
    "testing"
)

func TestToFloat64(t *testing.T) {
    values := []struct{dec Decimal; flt float64}{
        {Decimal{positive: true, prec: 1},
         0.0},
        {Decimal{positive: true, prec: 1, integer: [4]uint32{1}},
         1.0},
        {Decimal{positive: false, prec: 1, integer: [4]uint32{1}},
         -1.0},
        {Decimal{positive: true, prec: 1, scale: 1, integer: [4]uint32{5}},
         0.5},
    }
    for _, v := range values {
        if v.dec.ToFloat64() != v.flt {
            t.Error("values don't match ", v.dec.ToFloat64(), v.flt)
        }
    }
}
