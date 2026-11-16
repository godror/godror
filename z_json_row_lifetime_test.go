// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql/driver"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	godror "github.com/godror/godror"
)

// TestJSONRowLifetime checks that a JSON scanned from a row works until the
// next Next and returns ErrJSONClosed after its fetch buffer is gone,
// and that with KeepJSON it works until its own Close.
func TestJSONRowLifetime(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const qry = "SELECT JSON_OBJECT('n' VALUE LEVEL RETURNING JSON) FROM DUAL CONNECT BY LEVEL <= :1"
	check := func(t *testing.T, j godror.JSON, n string) {
		t.Helper()
		got, err := j.GetValue(godror.JSONOptNumberAsString)
		if err != nil {
			t.Fatalf("%s: %+v", n, err)
		}
		if d := cmp.Diff(map[string]any{"n": godror.Number(n)}, got); d != "" {
			t.Fatalf("%s: %s", n, d)
		}
		if jo, err := j.GetJSONObject(godror.JSONOptDefault); err != nil {
			t.Fatalf("%s: %+v", n, err)
		} else if jo.Len() != 1 {
			t.Fatalf("%s: got %d fields, wanted 1", n, jo.Len())
		}
	}
	checkClosed := func(t *testing.T, j godror.JSON, name string) {
		t.Helper()
		if _, err := j.GetValue(godror.JSONOptDefault); !errors.Is(err, godror.ErrJSONClosed) {
			t.Errorf("%s: GetValue: got %v, wanted ErrJSONClosed", name, err)
		}
		if _, err := j.GetJSONObject(godror.JSONOptDefault); !errors.Is(err, godror.ErrJSONClosed) {
			t.Errorf("%s: GetJSONObject: got %v, wanted ErrJSONClosed", name, err)
		}
	}

	// QueryRow closes the rows in Scan: the JSON must be kept.
	var j godror.JSON
	if err = conn.QueryRowContext(ctx, qry, 1, godror.KeepJSON()).Scan(&j); err != nil {
		var oraErr *godror.OraErr
		if errors.As(err, &oraErr) && (oraErr.Code() == 902 || oraErr.Code() == 40449) {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	check(t, j, "1")
	if got, want := j.String(), `{"n":1}`; got != want {
		t.Errorf("QueryRow: got %q, wanted %q", got, want)
	}
	for range 2 {
		if err = j.Close(); err != nil {
			t.Error(err)
		}
	}
	checkClosed(t, j, "after Close")
	if err = (godror.JSON{}).Close(); err != nil {
		t.Error(err)
	}
	var plain godror.JSON
	if err = conn.QueryRowContext(ctx, qry, 1).Scan(&plain); err != nil {
		t.Fatal(err)
	}
	checkClosed(t, plain, "QueryRow without KeepJSON")
	if err = plain.Close(); err != nil {
		t.Error(err)
	}

	// Three fetches of ten rows, with and without KeepJSON:
	// the values work in the loop, and after it only if they are kept.
	for _, keep := range []bool{false, true} {
		args := []any{30, godror.FetchArraySize(10)}
		if keep {
			args = append(args, godror.KeepJSON())
		}
		rows, err := conn.QueryContext(ctx, qry, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var all []godror.JSON
		for rows.Next() {
			var j godror.JSON
			if err = rows.Scan(&j); err != nil {
				t.Fatal(err)
			}
			all = append(all, j)
			check(t, j, strconv.Itoa(len(all)))
		}
		if err = rows.Close(); err != nil {
			t.Fatal(err)
		}
		if len(all) != 30 {
			t.Fatalf("got %d rows, wanted 30", len(all))
		}
		for i, j := range all {
			if !keep {
				checkClosed(t, j, "after the loop")
				continue
			}
			check(t, j, strconv.Itoa(i+1))
			if err = j.Close(); err != nil {
				t.Error(err)
			}
			checkClosed(t, all[i], "after Close")
		}
		if t.Failed() {
			t.FailNow()
		}
	}

	// The same through driver.Rows.Next, which database/sql uses before Go 1.27.
	var first godror.JSON
	if err = conn.Raw(func(dc any) error {
		stmt, err := dc.(driver.ConnPrepareContext).PrepareContext(ctx, qry)
		if err != nil {
			return err
		}
		defer stmt.Close()
		rows, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, []driver.NamedValue{{Ordinal: 1, Value: 2}})
		if err != nil {
			return err
		}
		defer rows.Close()
		dest := make([]driver.Value, 1)
		if err = rows.Next(dest); err != nil {
			return err
		}
		first = dest[0].(godror.JSON)
		check(t, first, "1")
		if err = rows.Next(dest); err != nil {
			return err
		}
		check(t, first, "1") // same fetch buffer
		if err = rows.Close(); err != nil {
			return err
		}
		checkClosed(t, first, "driver.Rows")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// A closed JSON cannot be bound.
	var s string
	if err = conn.QueryRowContext(ctx, "SELECT JSON_SERIALIZE(:1) FROM DUAL", first).Scan(&s); !errors.Is(err, godror.ErrJSONClosed) {
		t.Errorf("bind a closed JSON: got %v, wanted ErrJSONClosed", err)
	}
}
