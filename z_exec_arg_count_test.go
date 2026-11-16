// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestExecArgCount checks that an Exec with a wrong number of arguments fails,
// also when the same text was executed before with all of them: the statement
// cached for the text must not be executed with the values of the previous
// call.
func TestExecArgCount(t *testing.T) {
	ctx, cancel := testContext(t, 60*time.Second)
	defer cancel()
	tbl := "test_exec_argcnt" + tblSuffix
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.ExecContext(ctx, "DROP TABLE "+tbl+" PURGE")
	if _, err := conn.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(6), txt VARCHAR2(100))"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl + " PURGE")

	positional := "INSERT INTO " + tbl + " (id, txt) VALUES (:1, :2)"
	named := "INSERT INTO " + tbl + " (id, txt) VALUES (:id, :txt)"
	var out string
	for i, tC := range []struct {
		name, qry string
		// first has all the arguments, second is what is tested
		first, second []any
		// wantTxt is what the second call inserts; if it is empty, the call must fail
		wantTxt string
	}{
		{"fewer", positional, []any{1, "first"}, []any{2}, ""},
		{"none", positional, []any{1, "first"}, nil, ""},
		{"fewer and an Option", positional, []any{1, "first"}, []any{2, godror.BoolToString("Y", "N")}, ""},
		{"more", positional, []any{1, "first"}, []any{2, "second", "x"}, ""},
		{"same", positional, []any{1, "first"}, []any{2, "second"}, "second"},
		{"same and an Option", positional, []any{1, "first"}, []any{godror.BoolToString("Y", "N"), 2, true}, "Y"},
		{"name missing", named, []any{sql.Named("id", 1), sql.Named("txt", "first")}, []any{sql.Named("id", 2)}, ""},
		{"name more", named, []any{sql.Named("id", 1), sql.Named("txt", "first")}, []any{sql.Named("id", 2), sql.Named("txt", "second"), sql.Named("x", 3)}, ""},
		{"names in other order", named, []any{sql.Named("id", 1), sql.Named("txt", "first")}, []any{sql.Named("txt", "second"), sql.Named("id", 2)}, "second"},
		{"positional after named", named, []any{sql.Named("id", 1), sql.Named("txt", "first")}, []any{2}, ""},
		{"positional and named", "INSERT INTO " + tbl + " (id, txt) VALUES (:1, :txt)", []any{2, sql.Named("txt", "first")}, []any{sql.Named("txt", "second")}, ""},
		{"repeated name", "INSERT INTO " + tbl + " (id, txt) VALUES (:a, :a)", []any{1}, []any{2}, "2"},
		{"OUT", "BEGIN :1 := :2; INSERT INTO " + tbl + " (id, txt) VALUES (2, :2); END;", []any{sql.Out{Dest: &out}, "first"}, []any{sql.Out{Dest: &out}}, ""},
	} {
		t.Run(tC.name, func(t *testing.T) {
			if _, err := conn.ExecContext(ctx, "DELETE FROM "+tbl); err != nil {
				t.Fatal(err)
			}
			// A text of its own, to have a statement of its own in the cache.
			qry := tC.qry + fmt.Sprintf(" /* %d */", i)
			if _, err := conn.ExecContext(ctx, qry, tC.first...); err != nil {
				t.Fatal(err)
			}
			_, err := conn.ExecContext(ctx, qry, tC.second...)
			var got sql.NullString
			if qErr := conn.QueryRowContext(ctx, "SELECT MAX(txt) FROM "+tbl+" WHERE id = 2").Scan(&got); qErr != nil {
				t.Fatal(qErr)
			}
			if tC.wantTxt == "" {
				if !errors.Is(err, godror.ErrArgCount) {
					t.Errorf("got %v, wanted ErrArgCount; the call inserted %q", err, got.String)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.String != tC.wantTxt {
				t.Errorf("got %q, wanted %q", got.String, tC.wantTxt)
			}
		})
	}

	// Two users of the pool, one after the other.
	t.Run("pool", func(t *testing.T) {
		if _, err := conn.ExecContext(ctx, "DELETE FROM "+tbl); err != nil {
			t.Fatal(err)
		}
		qry := positional + " /* pool */"
		for i := 0; i < 20; i++ {
			if _, err := testDb.ExecContext(ctx, qry, 1, "of the other user"); err != nil {
				t.Fatal(err)
			}
			_, err := testDb.ExecContext(ctx, qry, 2)
			if !errors.Is(err, godror.ErrArgCount) {
				var got sql.NullString
				if qErr := testDb.QueryRowContext(ctx, "SELECT MAX(txt) FROM "+tbl+" WHERE id = 2").Scan(&got); qErr != nil {
					t.Fatal(qErr)
				}
				t.Fatalf("got %v, wanted ErrArgCount; the call inserted %q", err, got.String)
			}
		}
	})
}
