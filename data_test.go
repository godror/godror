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
	for _, want := range []time.Time{time.Now(), {}} {
		d.SetTime(want)
		if got := d.GetTime(); !got.Equal(want) && got.Format(time.RFC3339) != want.Format(time.RFC3339) {
			t.Errorf("set %v, got %v", want, got)
		}
	}

	for _, want := range []int8{-126, 126} {
		d.Set(want)
		if got := d.Get(); got.(int64) != int64(want) {
			t.Errorf("set %v, got %v", want, got)
		}
	}

	for _, want := range []int16{-30000, -15000, 0, 15000, 30000} {
		d.Set(want)
		if got := d.Get(); got.(int64) != int64(want) {
			t.Errorf("set %v, got %v", want, got)
		}
	}

	for _, want := range []uint8{0, 128, 253} {
		d.Set(want)
		if got := d.Get(); got.(uint64) != uint64(want) {
			t.Errorf("set %v, got %v", want, got)
		}
	}

	for _, want := range []uint16{0, 15000, 30000, 45000, 60000} {
		d.Set(want)
		if got := d.Get(); got.(uint64) != uint64(want) {
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

func BenchmarkDataGetBytes(b *testing.B) {
	var d Data
	d.SetBytes([]byte("árvíztűrő tükörfúrógép"))

	b.Run("C", func(b *testing.B) {
		b.ResetTimer()
		var n uint64
		for i := 0; i < b.N; i++ {
			b := d.dpiDataGetBytes()
			n += uint64(b.length)
		}
		b.Log("n:", n)
	})

	b.Run("unsafe", func(b *testing.B) {
		b.ResetTimer()
		var n uint64
		for i := 0; i < b.N; i++ {
			b := d.dpiDataGetBytesUnsafe()
			n += uint64(b.length)
		}
		b.Log("n:", n)
	})
}
