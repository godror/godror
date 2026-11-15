// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestCLOBReplacementChar checks that a U+FFFD character stored in a CLOB
// is returned by the LOB reader, even if a read ends with it,
// and that multibyte characters still survive the read boundaries.
func TestCLOBReplacementChar(t *testing.T) {
	ctx, cancel := testContext(t, 60*time.Second)
	defer cancel()
	tbl := "test_clob_fffd" + tblSuffix
	testDb.ExecContext(ctx, "DROP TABLE "+tbl+" PURGE")
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER, c CLOB, n NCLOB)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl+" PURGE")

	wants := []string{
		"abc�",
		"�",
		"ab�cd",
		strings.Repeat("�", 10),
		// bigger than one read; 1-4 byte characters and U+FFFD in the middle and at the end
		strings.Repeat("aáñ⅛\U0001F3B5�", 100000) + "�",
		// an odd offset, to have surrogate pairs on the read boundaries
		"a" + strings.Repeat("\U0001F3B5", 300000),
	}
	for i, want := range wants {
		if _, err := testDb.ExecContext(ctx, "INSERT INTO "+tbl+" (id, c, n) VALUES (:1, :2, :3)", i,
			godror.Lob{IsClob: true, Reader: strings.NewReader(want)},
			godror.Lob{IsClob: true, Reader: strings.NewReader(want)},
		); err != nil {
			t.Fatalf("insert %d: %+v", i, err)
		}
	}

	// 0 means io.ReadAll, which reads through the reader's internal buffer;
	// the others are read directly, 32772 with an odd number of characters per read.
	for _, bufSize := range []int{0, 32768, 32772} {
		func() {
			rows, err := testDb.QueryContext(ctx, "SELECT id, c, n FROM "+tbl+" ORDER BY id", godror.LobAsReader())
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			for rows.Next() {
				var id int
				var c, n any
				if err = rows.Scan(&id, &c, &n); err != nil {
					t.Fatal(err)
				}
				for name, v := range map[string]any{"CLOB": c, "NCLOB": n} {
					var sb strings.Builder
					if bufSize == 0 {
						var b []byte
						b, err = io.ReadAll(v.(*godror.Lob))
						sb.Write(b)
					} else {
						// hide WriteTo to read with our buffer size
						_, err = io.CopyBuffer(&sb, struct{ io.Reader }{v.(*godror.Lob).Reader}, make([]byte, bufSize))
					}
					if err != nil {
						t.Errorf("bufSize=%d id=%d %s: %+v", bufSize, id, name, err)
						continue
					}
					got, want := sb.String(), wants[id]
					if got == want {
						continue
					}
					if len(want) > 32 {
						got, want = got[max(0, len(got)-16):], want[len(want)-16:]
					}
					t.Errorf("bufSize=%d id=%d %s: got %d bytes (...%q), wanted %d (...%q)",
						bufSize, id, name, sb.Len(), got, len(wants[id]), want)
				}
			}
			if err = rows.Err(); err != nil {
				t.Error(err)
			}
		}()
	}
}
