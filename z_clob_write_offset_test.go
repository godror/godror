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
	"unicode/utf16"

	godror "github.com/godror/godror"
)

// shortReader returns at most n bytes per Read.
type shortReader struct {
	r io.Reader
	n int
}

func (r shortReader) Read(p []byte) (int, error) {
	if len(p) > r.n {
		p = p[:r.n]
	}
	return r.r.Read(p)
}

// TestCLOBWriteMultibyte writes non-ASCII text that does not fit into one
// write into CLOB and NCLOB columns. The CLOB writer used to advance the write
// offset by bytes, while ODPI-C expects it in characters.
func TestCLOBWriteMultibyte(t *testing.T) {
	ctx, cancel := testContext(t, 2*time.Minute)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tbl := "test_clobwo" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err = conn.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(3), c CLOB, nc NCLOB)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl)

	for _, tc := range []struct {
		name string
		s    string
	}{
		{"e_acute", strings.Repeat("é", 600_000)},
		{"a_e_acute", "a" + strings.Repeat("é", 600_000)},
		{"emoji", strings.Repeat("😀", 300_000)},
		{"a_emoji", "a" + strings.Repeat("😀", 300_000)},
		{"mix", strings.Repeat("aé€😀", 150_000)},
	} {
		// utf16.RuneLen counts the UTF-16 code units: 2 for supplementary characters.
		var want int
		for _, r := range tc.s {
			want += utf16.RuneLen(r)
		}
		for _, short := range []int{0, 7777} {
			name := tc.name
			if short != 0 {
				name += "_short"
			}
			t.Run(name, func(t *testing.T) {
				reader := func() io.Reader {
					if short == 0 {
						return strings.NewReader(tc.s)
					}
					return shortReader{r: strings.NewReader(tc.s), n: short}
				}
				if _, err := conn.ExecContext(ctx, "DELETE FROM "+tbl); err != nil {
					t.Fatal(err)
				}
				if _, err := conn.ExecContext(ctx, "INSERT INTO "+tbl+" (id, c, nc) VALUES (1, :1, :2)",
					godror.Lob{IsClob: true, Reader: reader()},
					godror.Lob{IsClob: true, Reader: reader()},
				); err != nil {
					t.Fatal(err)
				}
				var cLen, ncLen int
				var c, nc string
				if err := conn.QueryRowContext(ctx,
					"SELECT DBMS_LOB.GETLENGTH(c), DBMS_LOB.GETLENGTH(nc), c, nc FROM "+tbl+" WHERE id = 1",
				).Scan(&cLen, &ncLen, &c, &nc); err != nil {
					t.Fatal(err)
				}
				if cLen != want {
					t.Errorf("CLOB length: got %d, wanted %d", cLen, want)
				}
				if ncLen != want {
					t.Errorf("NCLOB length: got %d, wanted %d", ncLen, want)
				}
				if c != tc.s {
					t.Errorf("CLOB read back differs: got %d bytes, wanted %d", len(c), len(tc.s))
				}
				if nc != tc.s {
					t.Errorf("NCLOB read back differs: got %d bytes, wanted %d", len(nc), len(tc.s))
				}
			})
		}
	}
}
