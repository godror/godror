// Copyright 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"math/big"
	"strings"
)

func (N Number) Decompose(buf []byte) (form byte, negative bool, coefficient []byte, exponent int32) {
	s := string(N)
	mexp := strings.IndexByte(s, '.')
	if mexp >= 0 {
		s = s[:mexp] + s[mexp+1:]
		exponent = -int32(len(s) - mexp)
	}
	var i big.Int
	if _, ok := i.SetString(s, 10); !ok {
		return 2, false, nil, 0
	}
	switch i.Sign() {
	case 0:
		return 0, false, nil, 0
	case -1:
		negative = true
	}
	return 0, negative, i.FillBytes(buf), exponent
}
func (N *Number) Compose(form byte, negative bool, coefficient []byte, exponent int32) error {
	var i big.Int
	var start int
	length := 1 + len(coefficient)*3
	if exponent < 0 {
		start = 1
		length++
	} else if exponent > 0 {
		length += int(exponent)
	}
	p := make([]byte, start, length)
	if start != 0 {
		p[0] = '-'
	}
	i.SetBytes(coefficient)
	p = i.Append(p, 10)
	for ; exponent > 0; exponent-- {
		p = append(p, '0')
	}
	if exponent < 0 {
		exp := int(exponent)
		length = len(p) - start
		for ; exp < -length; exp++ {
			p = append(p[start:], '0')
		}
		p = append(p, ' ')
		copy(p[len(p)-exp:], p[len(p)-exp-1:len(p)-1])
		p[len(p)-exp] = '.'
	}
	*N = Number(string(p))
	return nil
}

var _ = decimal((*Number)(nil))

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
