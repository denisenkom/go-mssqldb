package mssql

// http://msdn.microsoft.com/en-us/library/ee780893.aspx
type Decimal struct {
    integer [4]uint32
    positive bool
    prec uint8
    scale uint8
}

func (d Decimal)ToFloat64() float64 {
    var val float64 = float64(d.integer[0])
    if !d.positive {
        val = -val
    }
    for i := 0; i < int(d.scale); i++ {
        val /= 10
    }
    return val
}

func StrToDecimal(s string) (Decimal, error) {
    return Decimal{}, nil
}

func Float32ToDecimal(f float32) Decimal {
    return Decimal{}
}
