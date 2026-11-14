// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"io"
	"strconv"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestLobReaderOutlivesFetch checks that the *Lob values returned with
// LobAsReader stay usable after the rows have moved on to the next fetch
// batch. They used to wrap the define variable's LOB without taking a
// reference, so ODPI-C freed them on the next fetch.
func TestLobReaderOutlivesFetch(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	const N = 30
	rows, err := testDb.QueryContext(ctx,
		"SELECT TO_CLOB(LEVEL) FROM DUAL CONNECT BY LEVEL <= "+strconv.Itoa(N),
		godror.LobAsReader(), godror.FetchArraySize(10))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	lobs := make([]*godror.Lob, 0, N)
	for rows.Next() {
		var l *godror.Lob
		if err := rows.Scan(&l); err != nil {
			t.Fatal(err)
		}
		lobs = append(lobs, l)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(lobs) != N {
		t.Fatalf("got %d rows, wanted %d", len(lobs), N)
	}
	for i, l := range lobs {
		b, err := io.ReadAll(l)
		if err != nil {
			t.Errorf("%d: %+v", i, err)
			continue
		}
		if got, want := string(b), strconv.Itoa(i+1); got != want {
			t.Errorf("%d: got %q, wanted %q", i, got, want)
		}
		if c, ok := l.Reader.(io.Closer); ok {
			if err := c.Close(); err != nil {
				t.Errorf("%d: close: %+v", i, err)
			}
		}
	}
}
