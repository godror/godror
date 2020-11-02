// Copyright 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package num

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"
)

// OCINum is an OCINumber
//
// SQLT_VNU: 22 bytes, at max.
// SQLT_NUM: 21 bytes
//
// http://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci03typ.htm#sthref365
//
// Oracle stores values of the NUMBER datatype in a variable-length format.
// The first byte is the exponent and is followed by 1 to 20 mantissa bytes.
// The high-order bit of the exponent byte is the sign bit; it is set for positive numbers and it is cleared for negative numbers.
// The lower 7 bits represent the exponent, which is a base-100 digit with an offset of 65.
//
// To calculate the decimal exponent, add 65 to the base-100 exponent and add another 128 if the number is positive.
// If the number is negative, you do the same, but subsequently the bits are inverted.
// For example, -5 has a base-100 exponent = 62 (0x3e). The decimal exponent is thus (~0x3e) -128 - 65 = 0xc1 -128 -65 = 193 -128 -65 = 0.
//
// Each mantissa byte is a base-100 digit, in the range 1..100.
// For positive numbers, the digit has 1 added to it. So, the mantissa digit for the value 5 is 6.
// For negative numbers, instead of adding 1, the digit is subtracted from 101.
// So, the mantissa digit for the number -5 is 96 (101 - 5).
// Negative numbers have a byte containing 102 appended to the data bytes.
// However, negative numbers that have 20 mantissa bytes do not have the trailing 102 byte.
// Because the mantissa digits are stored in base 100, each byte can represent 2 decimal digits.
// The mantissa is normalized; leading zeroes are not stored.
//
// Up to 20 data bytes can represent the mantissa. However, only 19 are guaranteed to be accurate.
// The 19 data bytes, each representing a base-100 digit, yield a maximum precision of 38 digits for an Oracle NUMBER.
//
// If you specify the datatype code 2 in the dty parameter of an OCIDefineByPos() call,
// your program receives numeric data in this Oracle internal format.
// The output variable should be a 21-byte array to accommodate the largest possible number.
// Note that only the bytes that represent the number are returned. There is no blank padding or NULL termination.
// If you need to know the number of bytes returned, use the VARNUM external datatype instead of NUMBER.
//
// So the number is stored as sign * significand * 100^exponent where significand is in 1.xxx format.
type OCINum []byte

var (
	ErrTooLong      = errors.New("input string too long")
	ErrNoDigit      = errors.New("no digit found")
	ErrBadCharacter = errors.New("bad character")
)

// IsNull returns whether the underlying number is NULL.
func (num OCINum) IsNull() bool { return len(num) < 2 }

// Print the number into the given byte slice.
//
//
func (num OCINum) Print(buf []byte) []byte {
	if len(num) == 0 {
		// NULL
		return buf[:0]
	}
	res := buf[:0]
	if bytes.Equal(num, []byte{128}) {
		// 0
		return append(res, '0')
	}
	if len(num) < 2 {
		// Can't be, must be NULL
		return buf[:0]
	}
	b, num := num[0], num[1:]
	negative := b&(1<<7) == 0
	exp := int(b) & 0x7f
	D := func(b byte) int64 { return int64(b - 1) }
	if negative {
		D = func(b byte) int64 { return int64(101 - b) }
		res = append(res, '-')
		exp = int((^b) & 0x7f)
		if num[len(num)-1] == 102 {
			num = num[:len(num)-1]
		}
	}
	exp -= 65

	var dotWritten bool
	digits := (*bytesPool.Get().(*[]byte))[:0]
	for i, b := range num {
		j := D(b)
		if j < 10 {
			digits = append(digits, '0', '0'+(byte(j)))
		} else {
			digits = strconv.AppendInt(digits, j, 10)
		}
		if i == exp {
			if len(digits) == 0 {
				digits = append(digits, '0')
			}
			digits = append(digits, '.')
			dotWritten = true
		}
	}
	for i := len(num) - 1; i < exp; i++ {
		digits = append(digits, '0', '0')
	}
	if !dotWritten {
		if exp < 0 {
			dexp := (-exp) << 1
			digits = append(make([]byte, dexp, dexp+len(digits)), digits...)
			digits[0] = '0'
			digits[1] = '.'
			for i := 2; i < dexp; i++ {
				digits[i] = '0'
			}
		} else {
			n := len(digits)
			if cap(digits) < n+1 {
				digits = append(digits, '0')
			} else {
				digits = digits[:n+1]
			}
			place := (exp + 1) << 1
			copy(digits[place+1:], digits[place:n])
			digits[place] = '.'
		}
		dotWritten = true
	}
	for len(digits) > 2 && digits[0] == '0' && digits[1] != '.' {
		digits = digits[1:]
	}
	res = append(res, digits...)
	bytesPool.Put(&digits)

	if dotWritten {
		for res[len(res)-1] == '0' {
			res = res[:len(res)-1]
		}
		if res[len(res)-1] == '.' {
			res = res[:len(res)-1]
		}
	}
	// 1	= 1		* 100^0
	// 10	= 10	* 100^0
	// 100	= 1		* 100^1
	// 1000	= 10	* 100^1
	// 0.1	= 10	* 100^-1
	// 0.01 = 1		* 100^-1
	// 0.001 = 10	* 100^-2
	// 0.0001 = 1	* 100^-2
	return res
}

var bytesPool = sync.Pool{New: func() interface{} { z := make([]byte, 0, 42); return &z }}

// String returns the string representation of the number.
func (num OCINum) String() string {
	b := *(bytesPool.Get().(*[]byte))
	s := string(num.Print(b))
	bytesPool.Put(&b)
	return s
}

// SetString sets the OCINum to the number in s.
func (num *OCINum) SetString(s string) error {
	s = strings.TrimSpace(s)
	if len(s) == 0 || s == "0" {
		*num = OCINum([]byte{128})
		return nil
	}
	var (
		dotSeen            bool
		nonZeros, numCount int
	)
	for i, r := range s {
		if '0' <= r && r <= '9' {
			numCount++
			if numCount == 40 {
				return fmt.Errorf("got %d, max 39 (%q): %w", numCount, s, ErrTooLong)
			}
			if r != '0' {
				nonZeros++
			}
			continue
		}
		if i == 0 && r == '-' {
			continue
		}
		if !dotSeen && r == '.' {
			dotSeen = true
			continue
		}
		return fmt.Errorf("%c in %q: %w", r, s, ErrBadCharacter)
	}
	if numCount == 0 {
		return fmt.Errorf("%s: %w", s, ErrNoDigit)
	}
	if nonZeros == 0 {
		*num = OCINum([]byte{128})
		return nil
	}

	// x = b - 1 <=> b = x + 1
	D := func(b byte) byte { return b + 1 }
	var negative bool
	if s[0] == '-' {
		negative = true
		s = s[1:]
		// x = 101 - b <=> b = 101 - x
		D = func(b byte) byte { return 101 - b }
	}
	i := len(s)
	if j := strings.IndexByte(s, '.'); j >= 0 {
		if j == 1 && s[0] == '0' {
			s = s[2:]
			i = 0
		} else {
			if j%2 != 0 {
				s = "0" + s
				j++
			}
			s = s[:j] + s[j+1:]
			i = j
		}
		if len(s)%2 == 1 {
			s = s + "0"
		}
	} else if len(s)%2 == 1 {
		s = "0" + s
		i = len(s)
	}

	for j := len(s) - 2; j > 0 && s[j] == '0' && s[j+1] == '0'; j -= 2 {
		s = s[:j]
	}
	exp := (i >> 1) - 1

	n := 1 + (len(s) >> 1) + 1
	if n > 21 {
		n = 21
	}
	if cap(*num) < n {
		*num = make([]byte, 1, n)
	} else {
		*num = (*num)[:1]
	}
	for i := 0; i < len(s)-1; i += 2 {
		b := 10*(s[i]-'0') + s[i+1] - '0'
		*num = append(*num, D(b))
	}
	exp += 65
	if negative {
		exp = (^exp) & 0x7f
		if n < 21 {
			*num = append(*num, 102)
		}
	} else {
		exp |= (1 << 7)
	}
	(*num)[0] = byte(exp)
	return nil
}

// Decompose returns the internal decimal state in parts.
// If the provided buf has sufficient capacity, buf may be returned as the coefficient with
// the value set and length set as appropriate.
func (num OCINum) Decompose(buf []byte) (form byte, negative bool, coefficient []byte, exponent int32) {
	if len(num) == 0 {
		// NULL
		return 2, false, nil, 0
	}
	if bytes.Equal(num, []byte{128}) {
		// 0
		return 0, false, []byte{0}, 0
	}
	if len(num) < 2 {
		// Can't be, must be NULL
		return 2, false, nil, 0
	}
	b, num := num[0], num[1:]
	negative = b&(1<<7) == 0
	exponent = int32(b & 0x7f)
	D := func(b byte) int64 { return int64(b - 1) }
	if negative {
		D = func(b byte) int64 { return int64(101 - b) }
		exponent = int32((^b) & 0x7f)
		if num[len(num)-1] == 102 {
			num = num[:len(num)-1]
		}
	}
	exponent -= 65

	i := big.NewInt(0)
	var a big.Int
	e := big.NewInt(1)
	hundred := big.NewInt(100)
	var zCount uint8
	for j := len(num) - 1; j >= 0; j-- {
		d := D(num[j])
		if d == 0 {
			zCount++
		} else {
			zCount = 0
		}
		i.Add(i, a.SetInt64(d).Mul(&a, e))
		if j != 0 {
			e.Mul(e, hundred)
		}
	}
	exponent -= int32(zCount)
	return 0, negative, i.Bytes(), exponent
}

var ErrOutOfRange = errors.New("out of range")

// Compose sets the internal decimal value from parts. If the value cannot be
// represented then an error should be returned.
func (num *OCINum) Compose(form byte, negative bool, coefficient []byte, exponent int32) error {
	if form != 0 {
		*num = []byte{}
		return nil
	}
	if len(coefficient) == 1 && coefficient[0] == 0 {
		*num = OCINum([]byte{128})
		return nil
	}
	if exponent > 0x7f {
		return fmt.Errorf("exponent=%d > 127: %w", exponent, ErrOutOfRange)
	}

	// x = b - 1 <=> b = x + 1
	D := func(b int64) byte { return byte(b + 1) }
	exp := byte(exponent + 65)
	if negative {
		exp = (^exp) & 0x7f
		D = func(b int64) byte { return byte(101 - b) }
	} else {
		exp |= (1 << 7)
	}
	res := (*num)[:0]
	res = append(res, exp)

	hundred := big.NewInt(100)
	var i, r big.Int
	i.SetBytes(coefficient)
	for {
		i.QuoRem(&i, hundred, &r)
		//fmt.Printf("i=%v r=%v\n", i, r)
		res = append(res, D(r.Int64()))
		if len(res) > 21 || len(i.Bits()) == 0 {
			break
		}
	}
	// reverse
	for i, j := 1, len(res)-1; i < j; i, j = i+1, j-1 {
		res[i], res[j] = res[j], res[i]
	}
	if negative && len(res) < 21 {
		res = append(res, 102)
	}
	*num = OCINum(res)

	return nil
}

var _ = decimal((*OCINum)(nil))

// decimal composes or decomposes a decimal value to and from individual parts.
// There are four parts: a boolean negative flag, a form byte with three possible states
// (finite=0, infinite=1, NaN=2), a base-2 big-endian integer
// coefficient (also known as a significand) as a []byte, and an int32 exponent.
// These are composed into a final value as "decimal = (neg) (form=finite) coefficient * 10 ^ exponent".
// A zero length coefficient is a zero value.
// The big-endian integer coefficient stores the most significant byte first (at coefficient[0]).
// If the form is not finite the coefficient and exponent should be ignored.
// The negative parameter may be set to true for any form, although implementations are not required
// to respect the negative parameter in the non-finite form.
//
// Implementations may choose to set the negative parameter to true on a zero or NaN value,
// but implementations that do not differentiate between negative and positive
// zero or NaN values should ignore the negative parameter without error.
// If an implementation does not support Infinity it may be converted into a NaN without error.
// If a value is set that is larger than what is supported by an implementation,
// an error must be returned.
// Implementations must return an error if a NaN or Infinity is attempted to be set while neither
// are supported.
//
// NOTE(kardianos): This is an experimental interface. See https://golang.org/issue/30870
type decimal interface {
	decimalDecompose
	decimalCompose
}

type decimalDecompose interface {
	// Decompose returns the internal decimal state in parts.
	// If the provided buf has sufficient capacity, buf may be returned as the coefficient with
	// the value set and length set as appropriate.
	Decompose(buf []byte) (form byte, negative bool, coefficient []byte, exponent int32)
}

type decimalCompose interface {
	// Compose sets the internal decimal value from parts. If the value cannot be
	// represented then an error should be returned.
	Compose(form byte, negative bool, coefficient []byte, exponent int32) error
}
