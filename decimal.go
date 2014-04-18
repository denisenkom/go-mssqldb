package mssql

import (
	"errors"
	"math"
	"math/big"
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

var factor1, factor2, factor3 big.Int

func init() {
	var acc float64 = 1
	for i := 0; i <= 38; i++ {
		scaletblflt64[i] = acc
		acc *= 10
	}
	factor1.Exp(big.NewInt(2), big.NewInt(32), nil)
	factor2.Exp(big.NewInt(2), big.NewInt(64), nil)
	factor3.Exp(big.NewInt(2), big.NewInt(96), nil)
}

func (d Decimal) Bytes() []byte {
	x := big.NewInt(int64(d.integer[0]))
	if d.integer[1] != 0 {
		y := big.NewInt(int64(d.integer[1]))
		y.Mul(y, &factor1)
		x.Add(x, y)
	}
	if d.integer[2] != 0 {
		y := big.NewInt(int64(d.integer[2]))
		y.Mul(y, &factor2)
		x.Add(x, y)
	}
	if d.integer[3] != 0 {
		y := big.NewInt(int64(d.integer[3]))
		y.Mul(y, &factor3)
		x.Add(x, y)
	}
	b := x.String()
	pos := len(b) - int(d.scale)
	var z []byte
	if !d.positive {
		z = append(z, byte('-'))
	}
	if pos <= 0 {
		z = append(z, byte('0'))
	} else if pos > 0 {
		z = append(z, []byte(b[:pos])...)
	}
	if d.scale > 0 {
		z = append(z, byte('.'))
		for pos < 0 {
			z = append(z, byte('0'))
			pos++
		}
		z = append(z, []byte(b[pos:])...)
	}
	return z
}

func (d Decimal) String() string {
	return string(d.Bytes())
}
