// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// A PL/SQL index-by table of INTERVAL DAY TO SECOND cannot be bound:
// []time.Duration with PlSQLArrays must be refused, not sent.
func TestIntervalPlSQLArray(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	pkg := "test_ivarr" + tblSuffix
	if _, err := testDb.ExecContext(ctx, `CREATE OR REPLACE PACKAGE `+pkg+` AS
  TYPE it IS TABLE OF INTERVAL DAY(9) TO SECOND(9) INDEX BY PLS_INTEGER;
  FUNCTION cnt(p IN it) RETURN PLS_INTEGER;
  PROCEDURE o(p OUT it);
  PROCEDURE io(p IN OUT it);
END;`); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP PACKAGE "+pkg)
	if _, err := testDb.ExecContext(ctx, `CREATE OR REPLACE PACKAGE BODY `+pkg+` AS
  FUNCTION cnt(p IN it) RETURN PLS_INTEGER IS BEGIN RETURN p.COUNT; END;
  PROCEDURE o(p OUT it) IS BEGIN p(1) := INTERVAL '1' SECOND; END;
  PROCEDURE io(p IN OUT it) IS BEGIN p(1) := p(1) * 2; END;
END;`); err != nil {
		t.Fatal(err)
	}

	var n int
	in := []time.Duration{time.Second, 25 * time.Hour}
	out := make([]time.Duration, 0, 3)
	inOut := append(make([]time.Duration, 0, 3), time.Second)
	for _, tc := range []struct {
		name, qry string
		args      []any
	}{
		{"IN", "BEGIN :1 := " + pkg + ".cnt(:2); END;", []any{sql.Out{Dest: &n}, in}},
		{"OUT", "BEGIN " + pkg + ".o(:1); END;", []any{sql.Out{Dest: &out}}},
		{"IN OUT", "BEGIN " + pkg + ".io(:1); END;", []any{sql.Out{Dest: &inOut, In: true}}},
	} {
		func() {
			conn, err := testDb.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_, err = conn.ExecContext(ctx, tc.qry, append([]any{godror.PlSQLArrays}, tc.args...)...)
			if !errors.Is(err, godror.ErrNotSupported) {
				t.Errorf("%s: got %v, wanted ErrNotSupported", tc.name, err)
			}
			var one int
			if err = conn.QueryRowContext(ctx, "SELECT 1 FROM DUAL").Scan(&one); err != nil {
				t.Errorf("%s: the connection is not usable: %v", tc.name, err)
			}
		}()
	}

	// Without PlSQLArrays a slice is a batch, and that works.
	if _, err := testDb.ExecContext(ctx,
		"BEGIN IF :1 <> INTERVAL '1' SECOND THEN RAISE_APPLICATION_ERROR(-20001, 'bad'); END IF; END;",
		[]time.Duration{time.Second, time.Second},
	); err != nil {
		t.Errorf("batch: %v", err)
	}
}
