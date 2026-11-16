// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestBindClosedLob binds a Lob that has been read to the end: the reader has
// released its LOB by then. That must return ErrLOBClosed and leave the
// connection, and its transaction, alone.
func TestBindClosedLob(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	tbl := "test_bind_closed_lob" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl+" PURGE")
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(6), c CLOB, c2 CLOB)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl+" PURGE")
	if _, err := testDb.ExecContext(ctx, "INSERT INTO "+tbl+" (id, c) VALUES (1, :1)",
		godror.Lob{IsClob: true, Reader: strings.NewReader(strings.Repeat("abc", 10000))},
	); err != nil {
		t.Fatal(err)
	}

	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO "+tbl+" (id) VALUES (99)"); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.QueryContext(ctx, "SELECT c FROM "+tbl+" WHERE id = 1", godror.LobAsReader())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	var c any
	if err = rows.Scan(&c); err != nil {
		t.Fatal(err)
	}
	lob := c.(*godror.Lob)
	if _, err = io.Copy(io.Discard, lob); err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+tbl+" SET c2 = :1 WHERE id = 1", *lob)
	if !errors.Is(err, godror.ErrLOBClosed) {
		t.Errorf("bind a Lob that was read to the end: got %v, wanted ErrLOBClosed", err)
	}
	if errors.Is(err, driver.ErrBadConn) {
		t.Error("got ErrBadConn")
	}
	rows.Close()
	var n int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(0) FROM "+tbl+" WHERE id = 99").Scan(&n); err != nil {
		t.Errorf("the transaction is lost: %v", err)
	} else if n != 1 {
		t.Errorf("the uncommitted row is lost: got %d rows, wanted 1", n)
	}
}
