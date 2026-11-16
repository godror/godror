// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestLobHijackConcurrentNext reads the DirectLob hijacked from a row's Lob
// in another goroutine while the loop goes on: Next closes that DirectLob,
// which must not race with the read (run with -race).
func TestLobHijackConcurrentNext(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	tbl := "test_lob_hijack_cc" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl+" PURGE")
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(6), b BLOB)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl+" PURGE")
	want := bytes.Repeat([]byte("0123456789abcdef"), 16384)
	for i := 0; i < 8; i++ {
		if _, err := testDb.ExecContext(ctx, "INSERT INTO "+tbl+" (id, b) VALUES (:1, :2)",
			i, godror.Lob{Reader: bytes.NewReader(want)},
		); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := testDb.QueryContext(ctx, "SELECT b FROM "+tbl+" ORDER BY id", godror.LobAsReader(), godror.FetchArraySize(3))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var wg sync.WaitGroup
	for rows.Next() {
		var b any
		if err = rows.Scan(&b); err != nil {
			t.Fatal(err)
		}
		dl, err := b.(*godror.Lob).Hijack()
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 8192)
			for off := int64(0); off < int64(len(want)); off += int64(len(buf)) {
				n, err := dl.ReadAt(buf, off)
				if err != nil {
					// Closed by the next Next.
					return
				}
				if !bytes.Equal(buf[:n], want[off:off+int64(n)]) {
					t.Errorf("ReadAt(%d): got wrong data (%d bytes)", off, n)
				}
			}
		}()
	}
	if err = rows.Err(); err != nil {
		t.Error(err)
	}
	rows.Close()
	wg.Wait()
}
