// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestCLOBTruncatedUTF8 binds CLOB data that ends in the middle of
// a multi-byte UTF-8 sequence, through a Reader.
// The incomplete sequence is dropped, as it is for a string bind.
func TestCLOBTruncatedUTF8(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	tbl := "test_clob_trunc" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(6), c CLOB)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl+" PURGE")

	// 100 bytes are bound as a string, 512 KiB or more through a temporary LOB.
	for i, size := range []int{100, 1 << 19, 1<<20 + 1} {
		for j, tail := range []string{"\xe2\x82", "\xe2", "\xf0\x9f\x98"} {
			id := i*10 + j
			data := strings.Repeat("x", size-1) + "é" + tail
			if _, err := testDb.ExecContext(ctx,
				"INSERT INTO "+tbl+" (id, c) VALUES (:1, :2)",
				id, godror.Lob{IsClob: true, Reader: strings.NewReader(data)},
			); err != nil {
				t.Errorf("size=%d tail=%x: %+v", size, tail, err)
				continue
			}
			var length int
			var last string
			if err := testDb.QueryRowContext(ctx,
				"SELECT DBMS_LOB.GETLENGTH(c), DBMS_LOB.SUBSTR(c, 2, DBMS_LOB.GETLENGTH(c)-1) FROM "+tbl+" WHERE id = :1", id,
			).Scan(&length, &last); err != nil {
				t.Fatal(err)
			}
			if length != size || last != "xé" {
				t.Errorf("size=%d tail=%x: got %d characters ending with %q, wanted %d ending with \"xé\"", size, tail, length, last, size)
			}
		}
	}
}
