// Copyright 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package num

import (
	"bytes"
	"errors"
	"fmt"
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
/*
Oracle stores values of the NUMBER datatype in a variable-length format. The first byte is the exponent and is followed by 1 to 20 mantissa bytes. The high-order bit of the exponent byte is the sign bit; it is set for positive numbers and it is cleared for negative numbers. The lower 7 bits represent the exponent, which is a base-100 digit with an offset of 65.

To calculate the decimal exponent, add 65 to the base-100 exponent and add another 128 if the number is positive. If the number is negative, you do the same, but subsequently the bits are inverted. For example, -5 has a base-100 exponent = 62 (0x3e). The decimal exponent is thus (~0x3e) -128 - 65 = 0xc1 -128 -65 = 193 -128 -65 = 0.

Each mantissa byte is a base-100 digit, in the range 1..100. For positive numbers, the digit has 1 added to it. So, the mantissa digit for the value 5 is 6. For negative numbers, instead of adding 1, the digit is subtracted from 101. So, the mantissa digit for the number -5 is 96 (101 - 5). Negative numbers have a byte containing 102 appended to the data bytes. However, negative numbers that have 20 mantissa bytes do not have the trailing 102 byte. Because the mantissa digits are stored in base 100, each byte can represent 2 decimal digits. The mantissa is normalized; leading zeroes are not stored.

Up to 20 data bytes can represent the mantissa. However, only 19 are guaranteed to be accurate. The 19 data bytes, each representing a base-100 digit, yield a maximum precision of 38 digits for an Oracle NUMBER.

If you specify the datatype code 2 in the dty parameter of an OCIDefineByPos() call, your program receives numeric data in this Oracle internal format. The output variable should be a 21-byte array to accommodate the largest possible number. Note that only the bytes that represent the number are returned. There is no blank padding or NULL termination. If you need to know the number of bytes returned, use the VARNUM external datatype instead of NUMBER.
*/
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
		return buf[:0]
	}
	res := buf[:0]
	if bytes.Equal(num, []byte{128}) {
		return append(res, '0')
	}
	if len(num) < 2 {
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
