package mssql


// http://msdn.microsoft.com/en-us/library/ee780893.aspx
type Decimal struct {
    integer [4]uint32
    positive bool
    prec uint8
    scale uint8
}

var scaletblflt64 [39]float64

func (d Decimal)ToFloat64() float64 {
    val := float64(0)
    for i := 3; i >= 0; i-- {
        val *= 0x100000000
        val += float64(d.integer[i])
    }
    if !d.positive {
        val = -val
    }
    if d.scale != 0 {
        val /= scaletblflt64[d.scale]
    }
    return val
}

func StrToDecimal(s string) (Decimal, error) {
    return Decimal{}, nil
}

func Float32ToDecimal(f float32) Decimal {
    return Decimal{}
}

func init() {
    var acc float64 = 1
    for i := 0; i <= 38; i++ {
        scaletblflt64[i] = acc
        acc *= 10
    }
}
