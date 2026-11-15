// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	godror "github.com/godror/godror"
	"github.com/godror/knownpb/timestamppb"
)

// A NULL element of an OUT PL/SQL array must arrive as a nil *Timestamp.
func TestOutTimestamppbSliceNull(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	pkg := "test_pbnull" + tblSuffix
	for _, qry := range []string{
		`CREATE OR REPLACE PACKAGE ` + pkg + ` AS
  TYPE dt_tab IS TABLE OF DATE INDEX BY PLS_INTEGER;
  PROCEDURE dt_out(p OUT dt_tab);
END;`,
		`CREATE OR REPLACE PACKAGE BODY ` + pkg + ` AS
  PROCEDURE dt_out(p OUT dt_tab) IS
  BEGIN
    p(1) := DATE '2026-01-02' + 0.5; p(2) := NULL; p(3) := DATE '2026-03-04' + 0.5;
  END;
END;`,
	} {
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Fatalf("%s: %+v", qry, err)
		}
	}
	defer testDb.ExecContext(context.Background(), "DROP PACKAGE "+pkg)

	out := make([]*timestamppb.Timestamp, 0, 5)
	if _, err := testDb.ExecContext(ctx,
		"BEGIN "+pkg+".dt_out(:1); END;",
		godror.PlSQLArrays, sql.Out{Dest: &out},
	); err != nil {
		t.Fatal(err)
	}
	if len(out) != 3 {
		t.Fatalf("got %d elements (%v), wanted 3", len(out), out)
	}
	for i, want := range []string{"2026-01-02", "", "2026-03-04"} {
		if want == "" {
			if out[i] != nil {
				t.Errorf("%d. got %v, wanted nil", i, out[i])
			}
		} else if got := out[i].AsTime().Format("2006-01-02"); got != want {
			t.Errorf("%d. got %s, wanted %s", i, got, want)
		}
	}
}
