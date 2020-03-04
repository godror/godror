// Copyright 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"testing"
	"time"
)

func TestDataSetGet(t *testing.T) {
	var d Data
	for _, want := range []time.Time{time.Now(), time.Time{}} {
		d.SetTime(want)
		if got := d.GetTime(); got != want && got.Format(time.RFC3339) != want.Format(time.RFC3339) {
			t.Errorf("set %v, got %v", want, got)
		}
	}

	for _, want := range []bool{true, false} {
		d.SetBool(want)
		if got := d.GetBool(); got != want {
			t.Errorf("set %v, got %v", want, got)
		}
	}

	for _, want := range []string{"árvíztűrő tükörfúrógép", "\x00", ""} {
		d.SetBytes([]byte(want))
		if got := string(d.GetBytes()); got != want {
			t.Errorf("set %v, got %v", want, got)
		}
	}

	for _, want := range []float64{3.14, -42} {
		d.SetFloat32(float32(want))
		if got := d.GetFloat32(); got != float32(want) {
			t.Errorf("set %v, got %v", want, got)
		}
		d.SetInt64(int64(want * 100))
		if got := d.GetInt64(); got != int64(want*100) {
			t.Errorf("set %v, got %v", want*100, got)
		}
		d.SetFloat64(want)
		if got := d.GetFloat64(); got != want {
			t.Errorf("set %v, got %v", want, got)
		}
	}
}
