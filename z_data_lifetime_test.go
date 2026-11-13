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
