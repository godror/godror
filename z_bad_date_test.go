// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestBadDateError checks that a time outside Oracle's date range gives
// ErrBadDate instead of a panic.
func TestBadDateError(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()

	bad := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	tooEarly := time.Date(-4713, 1, 1, 0, 0, 0, 0, time.UTC) // Oracle year -4714
	check := func(t *testing.T, what string, err error) {
		t.Helper()
		if !errors.Is(err, godror.ErrBadDate) {
			t.Errorf("%s: got %v, wanted ErrBadDate", what, err)
		}
	}

	t.Run("bind", func(t *testing.T) {
		var s string
		for _, v := range []any{bad, tooEarly, godror.NullTime{Valid: true, Time: bad}} {
			err := testDb.QueryRowContext(ctx, "SELECT TO_CHAR(:1) FROM DUAL", v).Scan(&s)
			check(t, "scalar", err)
		}
		_, err := testDb.ExecContext(ctx, "DECLARE v DATE := :1; BEGIN NULL; END;", []time.Time{time.Now(), bad})
		check(t, "slice", err)
		// The connection still works.
		if err := testDb.QueryRowContext(ctx, "SELECT TO_CHAR(:1, 'YYYY') FROM DUAL", time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)).Scan(&s); err != nil {
			t.Fatal(err)
		} else if s != "9999" {
			t.Errorf("got %q, wanted 9999", s)
		}
	})

	t.Run("Data", func(t *testing.T) {
		_, err := godror.NewData(bad)
		check(t, "NewData", err)
		var d godror.Data
		check(t, "Set", d.Set(bad))
	})

	t.Run("Object", func(t *testing.T) {
		typName := strings.ToUpper("test_bd_obj" + tblSuffix)
		testDb.ExecContext(ctx, "DROP TYPE "+typName)
		if _, err := testDb.ExecContext(ctx, "CREATE TYPE "+typName+" AS OBJECT (d DATE)"); err != nil {
			t.Fatal(err)
		}
		defer testDb.Exec("DROP TYPE " + typName)
		conn, err := testDb.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		typ, err := godror.GetObjectType(ctx, conn, typName)
		if err != nil {
			t.Fatal(err)
		}
		defer typ.Close()
		obj, err := typ.NewObject()
		if err != nil {
			t.Fatal(err)
		}
		defer obj.Close()
		check(t, "Object.Set", obj.Set("D", bad))
		if err := obj.Set("D", time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)); err != nil {
			t.Fatal(err)
		}
	})
}
