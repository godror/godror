// Copyright 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"errors"
	"testing"

	godror "github.com/godror/godror"
)

type nonFiniteDecimal struct {
	form     byte
	negative bool
}

func (d nonFiniteDecimal) Decompose([]byte) (byte, bool, []byte, int32) {
	return d.form, d.negative, nil, 0
}

func TestNumberRejectsNonFiniteDecimal(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value nonFiniteDecimal
	}{
		{"Infinity", nonFiniteDecimal{form: 1}},
		{"NegativeInfinity", nonFiniteDecimal{form: 1, negative: true}},
		{"NaN", nonFiniteDecimal{form: 2}},
		{"InvalidForm", nonFiniteDecimal{form: 255}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := godror.Number("7")
			if err := n.Compose(tc.value.Decompose(nil)); !errors.Is(err, godror.ErrInvalidNumber) {
				t.Errorf("Compose: got %q, %v; want ErrInvalidNumber", n, err)
			}
			if n != "7" {
				t.Errorf("Compose changed the destination to %q on error", n)
			}
			if err := n.Scan(tc.value); !errors.Is(err, godror.ErrInvalidNumber) {
				t.Errorf("Scan: got %q, %v; want ErrInvalidNumber", n, err)
			}
			if v, err := godror.Num.ConvertValue(tc.value); !errors.Is(err, godror.ErrInvalidNumber) {
				t.Errorf("ConvertValue: got %v, %v; want ErrInvalidNumber", v, err)
			}
		})
	}
}

func TestNumberDeCompose(t *testing.T) {
	p := make([]byte, 38)
	for i, s := range []string{
		"0",
		"1",
		"-2",
		"3.14",
		"-3.14",
		"1000",
		"3.456789",
		"0.01",
		"-0.09",
		"-0.89",
		"0.0000000001",
		"12345678901234567890123456789012345678",
	} {
		n := godror.Number(s)

		form, negative, coefficient, exponent := n.Decompose(p[:0])
		if want := s[0] == '-'; want != negative {
			t.Errorf("%d. Decompose(%q) got negative=%t, wanted %t", i, s, negative, want)
		}
		var m godror.Number
		if err := m.Compose(form, negative, coefficient, exponent); err != nil {
			t.Errorf("%d. cannot compose %c/%t/% x/%d from %q", i, form, negative, coefficient, exponent, s)
		}
		if string(m) != s {
			t.Errorf("%d. got %q wanted %q", i, n, s)
		}
	}
}
