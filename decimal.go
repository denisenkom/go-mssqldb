package mssql

import (
	"encoding/binary"
	"errors"
	"math"
	"math/big"
	"strings"
	"fmt"
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

const autoScale = 100

func Float64ToDecimal(f float64) (Decimal, error) {
	return Float64ToDecimalScale(f, autoScale)
}

func StringToDecimal(s string, dscale uint8) (Decimal, error) {
	var dec Decimal
	var intString string
	var scale int

	parts := strings.Split(s, ".")
	if len(parts) == 1 {
		// There is no decimal point, we can just parse the original string as an int
		intString = s
	} else if len(parts) == 2 {
		intString = parts[0] + parts[1]
		scale = len(parts[1])
	} else {
		return dec, fmt.Errorf("Can't convert %s to decimal: too many .s", s)
	}

	bigInt := new(big.Int)
	_, ok := bigInt.SetString(intString, 10)
	if !ok {
		return dec, fmt.Errorf("Can't convert %s to decimal", s)
	}

	prec := len(strings.TrimLeft(intString,"0"))
	if bigInt.Sign() < 0 {
		prec = prec - 1
	}

	if prec <= 0 {
		prec = 1
	}

	//https://docs.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql
	//38 is max precision
	if prec > 38 || scale > int(dscale) {
		return dec, fmt.Errorf("Decimal value %s too large", s)
	}

	var i, e = big.NewInt(10), big.NewInt(int64(dscale)-int64(scale))
	i.Exp(i, e, nil)
	bigInt.Mul(bigInt, i)

	dec.positive = bigInt.Sign() >= 0
	dec.prec = uint8(prec)
	dec.scale = uint8(dscale)

	bytes := bigInt.Bytes()
	for i, j := 0, len(bytes)-1; i < j; i, j = i+1, j-1 {
		bytes[i], bytes[j] = bytes[j], bytes[i]
	}
	if len(bytes) < 16 {
		bytes = append(bytes, make([]byte, 16-len(bytes))...)
	} else if len(bytes) > 16 {
		return dec, fmt.Errorf("Decimal value %s too large", s)
	}

	dec.integer[0] = binary.LittleEndian.Uint32(bytes[0:4])
	dec.integer[1] = binary.LittleEndian.Uint32(bytes[4:8])
	dec.integer[2] = binary.LittleEndian.Uint32(bytes[8:12])
	dec.integer[3] = binary.LittleEndian.Uint32(bytes[12:16])

	return dec, nil
}

func Float64ToDecimalScale(f float64, scale uint8) (Decimal, error) {
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
	for dec.scale = 0; dec.scale <= scale; dec.scale++ {
		integer = f * scaletblflt64[dec.scale]
		_, frac := math.Modf(integer)
		if frac == 0 && scale == autoScale {
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

func (d Decimal) BigInt() big.Int {
	bytes := make([]byte, 16)
	binary.BigEndian.PutUint32(bytes[0:4], d.integer[3])
	binary.BigEndian.PutUint32(bytes[4:8], d.integer[2])
	binary.BigEndian.PutUint32(bytes[8:12], d.integer[1])
	binary.BigEndian.PutUint32(bytes[12:16], d.integer[0])
	var x big.Int
	x.SetBytes(bytes)
	if !d.positive {
		x.Neg(&x)
	}
	return x
}

func (d Decimal) Bytes() []byte {
	x := d.BigInt()
	return scaleBytes(x.String(), d.scale)
}

func (d Decimal) UnscaledBytes() []byte {
	x := d.BigInt()
	return x.Bytes()
}

func scaleBytes(s string, scale uint8) []byte {
	z := make([]byte, 0, len(s)+1)
	if s[0] == '-' || s[0] == '+' {
		z = append(z, byte(s[0]))
		s = s[1:]
	}
	pos := len(s) - int(scale)
	if pos <= 0 {
		z = append(z, byte('0'))
	} else if pos > 0 {
		z = append(z, []byte(s[:pos])...)
	}
	if scale > 0 {
		z = append(z, byte('.'))
		for pos < 0 {
			z = append(z, byte('0'))
			pos++
		}
		z = append(z, []byte(s[pos:])...)
	}
	return z
}

func (d Decimal) String() string {
	return string(d.Bytes())
}
