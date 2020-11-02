// Copyright 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"testing"

	godror "github.com/godror/godror"
)

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
