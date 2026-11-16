// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"errors"
	"io"
	"strconv"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestLobOutQuery checks that the OUT Lob of a PL/SQL block executed with
// Query is readable while the rows are open, as it is after Exec.
func TestLobOutQuery(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	const qry = "BEGIN :1 := TO_CLOB('value-'||:2); END;"
	stmt, err := conn.PrepareContext(ctx, qry)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var prev godror.Lob
	for i, mode := range []string{"conn", "prepared", "prepared"} {
		lob := godror.Lob{IsClob: true}
		var rows *sql.Rows
		if mode == "conn" {
			rows, err = conn.QueryContext(ctx, qry, sql.Out{Dest: &lob}, i)
		} else {
			rows, err = stmt.QueryContext(ctx, sql.Out{Dest: &lob}, i)
		}
		if err != nil {
			t.Fatalf("%d. %s: %+v", i, mode, err)
		}
		if lob.Reader == nil {
			t.Errorf("%d. %s: the OUT Lob is not set", i, mode)
		} else if b, err := io.ReadAll(lob); err != nil {
			t.Errorf("%d. %s: read the OUT Lob: %+v", i, mode, err)
		} else if want := "value-" + strconv.Itoa(i); string(b) != want {
			t.Errorf("%d. %s: got %q, wanted %q", i, mode, b, want)
		}
		rows.Close()
		if i == 2 {
			// The Lob of the previous execution of the same statement.
			var a [8]byte
			if _, err := prev.Read(a[:]); !errors.Is(err, godror.ErrLOBClosed) {
				t.Errorf("Read after the next Query: got %v, wanted ErrLOBClosed", err)
			}
		}
		prev = godror.Lob{IsClob: true}
		if mode == "prepared" {
			rows, err = stmt.QueryContext(ctx, sql.Out{Dest: &prev}, i)
			if err != nil {
				t.Fatal(err)
			}
			rows.Close()
		}
	}
}
