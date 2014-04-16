package mssql

import (
	"errors"
	"math"
)

// http://msdn.microsoft.com/en-us/library/ee780893.aspx
type Decimal struct {
	integer  [4]uint32
	positive bool
	prec     uint8
	scale    uint8
}

var scaletblflt64 [39]float64

func (d Decimal) ToFloat64() float64 {
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

func Float64ToDecimal(f float64) (Decimal, error) {
	var dec Decimal
	if math.IsNaN(f) {
		return dec, errors.New("NaN")
	}
	if math.IsInf(f, 0) {
		return dec, errors.New("Infinity can't be converted to decimal")
	}
	dec.positive = f >= 0
	if !dec.positive {
		f = math.Abs(f)
	}
	if f > 3.402823669209385e+38 {
		return dec, errors.New("Float value is out of range")
	}
	dec.prec = 20
	var integer float64
	for dec.scale = 0; dec.scale <= 20; dec.scale++ {
		integer = f * scaletblflt64[dec.scale]
		_, frac := math.Modf(integer)
		if frac == 0 {
			break
		}
	}
	for i := 0; i < 4; i++ {
		mod := math.Mod(integer, 0x100000000)
		integer -= mod
		integer /= 0x100000000
		dec.integer[i] = uint32(mod)
	}
	return dec, nil
}

func init() {
	var acc float64 = 1
	for i := 0; i <= 38; i++ {
		scaletblflt64[i] = acc
		acc *= 10
	}
}
