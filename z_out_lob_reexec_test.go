// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"fmt"
	"io"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestOutLobPreparedReexec executes a prepared statement with an OUT Lob
// several times: each execution must return a readable Lob with its own
// value, whether the Lob of the previous execution was read to the end,
// closed, or left unread.
func TestOutLobPreparedReexec(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, "BEGIN :1 := TO_CLOB('value-'||:2); END;")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < 300; i++ {
		lob := godror.Lob{IsClob: true}
		if _, err := stmt.ExecContext(ctx, sql.Out{Dest: &lob}, i); err != nil {
			t.Fatalf("%d. %v", i, err)
		}
		switch i % 3 {
		case 0, 1:
			b, err := io.ReadAll(lob)
			if err != nil {
				t.Fatalf("%d. read: %v", i, err)
			}
			if want := fmt.Sprintf("value-%d", i); string(b) != want {
				t.Errorf("%d. got %q, wanted %q", i, b, want)
			}
		case 2:
			if i%2 == 0 {
				if err := lob.Reader.(io.Closer).Close(); err != nil {
					t.Errorf("%d. Close: %v", i, err)
				}
			}
		}
	}
}
