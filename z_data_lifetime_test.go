// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestObjectNumberAttrGC reads NUMBER attributes of an object while the GC
// runs and other goroutines reuse freed memory. The buffer ODPI-C writes the
// number text into used to be a local array that nothing kept alive.
func TestObjectNumberAttrGC(t *testing.T) {
	ctx, cancel := testContext(t, 60*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	const typName = "TEST_NUMATTR_OT"
	conn.ExecContext(ctx, "DROP TYPE "+typName)
	if _, err = conn.ExecContext(ctx, "CREATE OR REPLACE TYPE "+typName+" AS OBJECT (n1 NUMBER, n2 NUMBER, n3 NUMBER)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TYPE "+typName)

	ot, err := godror.GetObjectType(ctx, conn, typName)
	if err != nil {
		t.Fatal(err)
	}
	defer ot.Close()
	obj, err := ot.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	want := map[string]string{"N1": "123456789.123456789", "N2": "-42.5", "N3": "31415926535897932384626433"}
	for k, v := range want {
		if err = obj.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var sink [][]byte
			for {
				select {
				case <-stop:
					return
				default:
				}
				b := make([]byte, 39)
				for j := range b {
					b[j] = 'X'
				}
				sink = append(sink, b)
				if len(sink) > 1000 {
					sink = sink[:0]
					runtime.GC()
				}
			}
		}()
	}
	defer func() { close(stop); wg.Wait() }()

	for i := 0; i < 20000; i++ {
		for k, v := range want {
			got, err := obj.Get(k)
			if err != nil {
				t.Fatal(err)
			}
			if s := fmt.Sprintf("%v", got); s != v {
				t.Fatalf("%d. %s: got %q, wanted %q", i, k, s, v)
			}
		}
	}
}

// TestConnNewData uses the Data of conn.NewData as input and output of
// object attributes. NewData releases its dpiVar before returning,
// so nothing may read the variable's buffers through the Data.
func TestConnNewData(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	typ := "test_conn_newdata_t" + tblSuffix
	if _, err := testDb.ExecContext(ctx, "CREATE OR REPLACE TYPE "+typ+" AS OBJECT (s VARCHAR2(100), n NUMBER, r RAW(100), c CLOB)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TYPE "+typ)
	cx, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cx.Close()
	if err := cx.Raw(func(driverConn any) error {
		conn := driverConn.(godror.Conn)
		ot, err := conn.GetObjectType(typ)
		if err != nil {
			return err
		}
		defer ot.Close()
		obj, err := ot.NewObject()
		if err != nil {
			return err
		}
		defer obj.Close()
		for _, tC := range []struct {
			base, val any
			attr      string
		}{
			{base: "", attr: "S", val: "string value"},
			{base: []byte("abc"), attr: "R", val: []byte("raw value")},
			{base: godror.Number("1"), attr: "N", val: "123.5"},
			{base: int64(0), attr: "N", val: int64(42)},
			{base: godror.Lob{IsClob: true}, attr: "C"},
		} {
			data, err := conn.NewData(tC.base, 2, 100)
			if err != nil {
				return fmt.Errorf("%T: %w", tC.base, err)
			}
			runtime.GC()
			for i, d := range data {
				if got := d.Get(); !d.IsNull() || d.GetLob() != nil || len(d.GetBytes()) != 0 {
					t.Errorf("%T[%d]: got %v, wanted NULL", tC.base, i, got)
				}
			}
			if err := obj.SetAttribute(tC.attr, data[0]); err != nil {
				t.Errorf("%T: set NULL: %v", tC.base, err)
			}
			if err := obj.GetAttribute(data[1], tC.attr); err != nil || !data[1].IsNull() {
				t.Errorf("%T: got %v (%v), wanted NULL", tC.base, data[1].Get(), err)
			}
			if tC.val == nil {
				continue
			}
			if err := data[1].Set(tC.val); err != nil {
				return fmt.Errorf("%T: %w", tC.base, err)
			}
			if err := obj.SetAttribute(tC.attr, data[1]); err != nil {
				return fmt.Errorf("%T: %w", tC.base, err)
			}
			if err := obj.GetAttribute(data[0], tC.attr); err != nil {
				return fmt.Errorf("%T: %w", tC.base, err)
			}
			got, want := data[0].Get(), tC.val
			if b, ok := got.([]byte); ok {
				got = string(b)
			}
			if b, ok := want.([]byte); ok {
				want = string(b)
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("%T: got %v, wanted %v", tC.base, got, want)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
