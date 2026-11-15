// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// An Option given as an argument of one execution of a prepared statement
// must not stay in effect for the later executions.
func TestExecOptionNotSticky(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, "SELECT 1.5 FROM DUAL")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for i, tC := range []struct {
		opts      []any
		wantFloat bool
	}{
		{nil, false},
		{[]any{godror.NumberAsFloat64()}, true},
		{nil, false},
	} {
		var v any
		if err := stmt.QueryRowContext(ctx, tC.opts...).Scan(&v); err != nil {
			t.Fatalf("%d. %+v", i, err)
		}
		if _, isFloat := v.(float64); isFloat != tC.wantFloat {
			t.Errorf("%d. got %T with options %v", i, v, tC.opts)
		}
	}
}
