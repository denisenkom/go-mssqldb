package decimal

import (
	"math"
	"testing"
)

func TestToString(t *testing.T) {
	values := []struct {
		dec Decimal
		s   string
	}{
		{Decimal{positive: true, prec: 10, scale: 0, integer: [4]uint32{1, 0, 0, 0}}, "1"},
		{Decimal{positive: false, prec: 10, scale: 0, integer: [4]uint32{1, 0, 0, 0}}, "-1"},
		{Decimal{positive: true, prec: 10, scale: 1, integer: [4]uint32{1, 0, 0, 0}}, "0.1"},
		{Decimal{positive: true, prec: 10, scale: 2, integer: [4]uint32{1, 0, 0, 0}}, "0.01"},
		{Decimal{positive: false, prec: 10, scale: 1, integer: [4]uint32{1, 0, 0, 0}}, "-0.1"},
		{Decimal{positive: true, prec: 10, scale: 2, integer: [4]uint32{100, 0, 0, 0}}, "1.00"},
		{Decimal{positive: false, prec: 10, scale: 2, integer: [4]uint32{100, 0, 0, 0}}, "-1.00"},
		{Decimal{positive: true, prec: 30, scale: 0, integer: [4]uint32{0, 1, 0, 0}}, "4294967296"},           // 2^32
		{Decimal{positive: true, prec: 30, scale: 0, integer: [4]uint32{0, 0, 1, 0}}, "18446744073709551616"}, // 2^64
		{Decimal{positive: true, prec: 30, scale: 0, integer: [4]uint32{0, 1, 1, 0}}, "18446744078004518912"}, // 2^64+2^32
	}
	for _, v := range values {
		if v.dec.String() != v.s {
			t.Error("String values don't match ", v.dec.String(), v.s)
		}
	}
}

func TestToFloat64(t *testing.T) {
	values := []struct {
		dec Decimal
		flt float64
	}{
		{Decimal{positive: true, prec: 1},
			0.0},
		{Decimal{positive: true, prec: 1, integer: [4]uint32{1}},
			1.0},
		{Decimal{positive: false, prec: 1, integer: [4]uint32{1}},
			-1.0},
		{Decimal{positive: true, prec: 1, scale: 1, integer: [4]uint32{5}},
			0.5},
		{Decimal{positive: true, prec: 38, integer: [4]uint32{0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff}},
			3.402823669209385e+38},
		{Decimal{positive: true, prec: 38, scale: 3, integer: [4]uint32{0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff}},
			3.402823669209385e+35},
	}
	for _, v := range values {
		if v.dec.ToFloat64() != v.flt {
			t.Error("ToFloat values don't match ", v.dec.ToFloat64(), v.flt)
		}
	}
}

func TestFromFloat64(t *testing.T) {
	values := []struct {
		dec Decimal
		flt float64
	}{
		{Decimal{positive: true, prec: 20},
			0.0},
		{Decimal{positive: true, prec: 20, integer: [4]uint32{1}},
			1.0},
		{Decimal{positive: false, prec: 20, integer: [4]uint32{1}},
			-1.0},
		{Decimal{positive: true, prec: 20, scale: 1, integer: [4]uint32{5}},
			0.5},
		{Decimal{positive: true, prec: 20, integer: [4]uint32{0, 0, 0xfffff000, 0xffffffff}},
			3.402823669209384e+38},
		//{Decimal{positive: true, prec: 20, scale: 3, integer: [4]uint32{0, 0, 0xfffff000, 0xffffffff}},
		// 3.402823669209385e+35},
	}
	for _, v := range values {
		decfromflt, err := Float64ToDecimal(v.flt)
		if err == nil {
			if decfromflt != v.dec {
				t.Error("FromFloat values don't match ", decfromflt, v.dec)
			}
		} else {
			t.Error("Float64ToDecimal failed with error:", err.Error())
		}
	}

	_, err := Float64ToDecimal(math.NaN())
	if err == nil {
		t.Error("Expected to get error for conversion from NaN, but didn't")
	}

	_, err = Float64ToDecimal(math.Inf(1))
	if err == nil {
		t.Error("Expected to get error for conversion from positive infinity, but didn't")
	}

	_, err = Float64ToDecimal(math.Inf(-1))
	if err == nil {
		t.Error("Expected to get error for conversion from negative infinity, but didn't")
	}
	_, err = Float64ToDecimal(3.402823669209386e+38)
	if err == nil {
		t.Error("Expected to get error for conversion from too big number, but didn't")
	}
	_, err = Float64ToDecimal(-3.402823669209386e+38)
	if err == nil {
		t.Error("Expected to get error for conversion from too big number, but didn't")
	}
}

func TestFromInt64(t *testing.T) {
	values := []struct {
		in    int64
		scale uint8
		out   string
	}{
		{0, 0, "0"},
		{12345, 3, "12.345"},
		{math.MaxInt64, 0, "9223372036854775807"},
		{math.MinInt64, 0, "-9223372036854775808"},
		{-100, 0, "-100"},
	}
	for _, v := range values {
		dec := Int64ToDecimalScale(v.in, v.scale)
		if dec.String() != v.out {
			t.Error("Int64ToDecimalScale values don't match ", v.in, dec, v.scale, v.out)
		}
	}
}

func TestFromString(t *testing.T) {
	values := []struct {
		in    string
		scale uint8
		out   string
	}{
		{"0", 0, "0"},
		{"-000.000", 3, "0.000"},
		{"-000.000", 5, "0.00000"},
		{"1", 0, "1"},
		{"-01.0", 1, "-1.0"},
		{"0.01", 2, "0.01"},
		{"-0.1", 10, "-0.1000000000"},
		{"1.00", 2, "1.00"},
		{"-1.00", 2, "-1.00"},
		{"4294967296", 0, "4294967296"},
		{"18446744073709551616", 0, "18446744073709551616"},
		{"18446744078004.518912", 6, "18446744078004.518912"},
		{"-18446744078004518912.12345", 5, "-18446744078004518912.12345"},
		{"1844674407800451891212345", 0, "1844674407800451891212345"},
		{"79228162532711081671548469248", 2, "79228162532711081671548469248.00"},
	}
	for _, v := range values {
		dec, err := StringToDecimalScale(v.in, v.scale)
		if err != nil {
			t.Error("StringToDecimal failed with error:", err.Error())
		} else if dec.String() != v.out {
			t.Error("StringToDecimal values don't match ", v.in, dec.String(), v.out)
		}
	}
}

func TestFromStringBad(t *testing.T) {
	arr := make([]rune, 256)
	for i := range arr {
		arr[i] = '0'
	}
	bigScaleNumber := "0." + string(arr) + "1"

	values := []struct {
		in    string
		scale uint8
	}{
		{"0.0001", 2},
		{bigScaleNumber, 2},
		{"not a number", 2},
		{"400000000000000000000000000000000000000", 2},
	}
	for _, v := range values {
		_, err := StringToDecimalScale(v.in, v.scale)
		if err == nil {
			t.Error("expected to fail but it didn't")
		}
	}
}
