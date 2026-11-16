// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestJSONCloseRace checks that a JSON can be read while another goroutine
// closes it: database/sql closes the rows from its own goroutine when the
// context of the query is cancelled; JSON.Close does it for a kept JSON.
// The readers must get the document or ErrJSONClosed.
// (Without the fix it crashes, or -race and -asan report it.)
func TestJSONCloseRace(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	// Big enough for a read to take a few milliseconds.
	const qry = `SELECT JSON_ARRAYAGG(RPAD('x'||n, 100, 'y') ORDER BY n RETURNING JSON)
		FROM (SELECT LEVEL n FROM DUAL CONNECT BY LEVEL <= 20000)`

	// read reads j from two goroutines until it is closed.
	read := func(t *testing.T, j godror.JSON) {
		t.Helper()
		var wg sync.WaitGroup
		for g := range 2 {
			wg.Go(func() {
				for ctx.Err() == nil {
					var n int
					var err error
					if g == 0 {
						var v any
						if v, err = j.GetValue(godror.JSONOptDefault); err == nil {
							n = len(v.([]any))
						}
					} else {
						var s string
						if s, err = j.StringWithOption(godror.JSONOptNumberAsString); err == nil {
							n = len(s) / 103 // "x…y",
						}
					}
					if err != nil {
						if !errors.Is(err, godror.ErrJSONClosed) {
							t.Errorf("got %v, wanted ErrJSONClosed", err)
						}
						return
					}
					if n != 20000 {
						t.Errorf("got %d elements without an error, wanted 20000", n)
						return
					}
				}
				t.Error("the JSON was not closed")
			})
		}
		wg.Wait()
	}

	for i := range 10 {
		delay := time.Duration(i%5) * 3 * time.Millisecond

		// The context of the query is cancelled.
		qctx, qcancel := context.WithCancel(ctx)
		rows, err := testDb.QueryContext(qctx, qry)
		if err != nil {
			qcancel()
			var oraErr *godror.OraErr
			if errors.As(err, &oraErr) && (oraErr.Code() == 902 || oraErr.Code() == 40449) {
				t.Skip(err)
			}
			t.Fatal(err)
		}
		if !rows.Next() {
			qcancel()
			t.Fatal(rows.Err())
		}
		var j godror.JSON
		if err = rows.Scan(&j); err != nil {
			qcancel()
			t.Fatal(err)
		}
		time.AfterFunc(delay, qcancel)
		read(t, j)
		rows.Close()

		// A kept JSON is closed.
		if err = testDb.QueryRowContext(ctx, qry, godror.KeepJSON()).Scan(&j); err != nil {
			t.Fatal(err)
		}
		time.AfterFunc(delay, func() { j.Close() })
		read(t, j)
		if t.Failed() {
			break
		}
	}
}
