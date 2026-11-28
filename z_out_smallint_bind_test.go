// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestOutSmallIntNoCursorLeak reads values that are not integers of the
// destination's kind into small integer OUT binds, many times on one session:
// each must fail, and must not use up the session's cursors (ORA-01000).
func TestOutSmallIntNoCursorLeak(t *testing.T) {
	ctx, cancel := testContext(t, 2*time.Minute)
	defer cancel()
	// More than open_cursors, which is 300 by default.
	const iterations = 1000
	for _, tc := range []struct {
		name string
		dest func() any
		qry  string
	}{
		{"int32<-0.5", func() any { return new(int32) }, "BEGIN SELECT 0.5 INTO :1 FROM DUAL; END;"},
		{"int16<-1e30", func() any { return new(int16) }, "BEGIN :1 := 1e30; END;"},
		{"uint16<--1", func() any { return new(uint16) }, "BEGIN SELECT -1 INTO :1 FROM DUAL; END;"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := testDb.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			for i := range iterations {
				_, err := conn.ExecContext(ctx, tc.qry, sql.Out{Dest: tc.dest()})
				if err == nil {
					t.Fatal("wanted an error")
				}
				var oraErr *godror.OraErr
				if errors.As(err, &oraErr) && oraErr.Code() == 1000 {
					// Do not give the session without cursors back to the pool.
					conn.Raw(func(any) error { return driver.ErrBadConn })
					t.Fatalf("after %d failed executions: %+v", i, err)
				}
			}
			// The session is still usable, and a good value arrives.
			var n int32
			if _, err := conn.ExecContext(ctx, "BEGIN SELECT 42 INTO :1 FROM DUAL; END;", sql.Out{Dest: &n}); err != nil {
				t.Fatal(err)
			} else if n != 42 {
				t.Errorf("got %d, wanted 42", n)
			}
		})
	}
}
