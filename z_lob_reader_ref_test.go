// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestLobRowLifetime checks that a Lob of a row (LobAsReader) is readable
// till the next Next or Close of the rows, and returns ErrLOBClosed after
// that, instead of using the LOB that the define variable has freed.
func TestLobRowLifetime(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tbl := "test_lobrow" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl+" PURGE")
	if _, err = conn.ExecContext(ctx, "CREATE TABLE "+tbl+" (id NUMBER(3), c CLOB, b BLOB, c2 CLOB)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TABLE "+tbl+" PURGE")

	const N = 25
	text := func(i int) string {
		if i == 3 { // > 1 MiB
			return strings.Repeat("0123456789abcdef", 100000)
		}
		return "clob-" + strconv.Itoa(i)
	}
	for i := 0; i < N; i++ {
		if _, err = conn.ExecContext(ctx, "INSERT INTO "+tbl+" (id, c, b, c2) VALUES (:1, :2, :3, :4)", i,
			godror.Lob{IsClob: true, Reader: strings.NewReader(text(i))},
			godror.Lob{Reader: strings.NewReader(text(i))},
			"second-"+strconv.Itoa(i),
		); err != nil {
			t.Fatal(err)
		}
	}
	const qry = "SELECT c, b, c2 FROM "

	// wantClosed checks that every use of the dead Lob returns ErrLOBClosed.
	wantClosed := func(t *testing.T, when string, lobs ...*godror.Lob) {
		t.Helper()
		var a [8]byte
		for _, lob := range lobs {
			if _, err := lob.Read(a[:]); !errors.Is(err, godror.ErrLOBClosed) {
				t.Errorf("%s: Read: got %v, wanted ErrLOBClosed", when, err)
			}
			if _, err := lob.ReadAt(a[:], 0); !errors.Is(err, godror.ErrLOBClosed) {
				t.Errorf("%s: ReadAt: got %v, wanted ErrLOBClosed", when, err)
			}
			if _, err := lob.Size(); !errors.Is(err, godror.ErrLOBClosed) {
				t.Errorf("%s: Size: got %v, wanted ErrLOBClosed", when, err)
			}
			if _, err := conn.ExecContext(ctx, "BEGIN :1 := DBMS_LOB.getlength(:2); END;", sql.Out{Dest: new(int64)}, *lob); !errors.Is(err, godror.ErrLOBClosed) {
				t.Errorf("%s: bind: got %v, wanted ErrLOBClosed", when, err)
			}
			if _, err := lob.Hijack(); !errors.Is(err, godror.ErrLOBClosed) {
				t.Errorf("%s: Hijack: got %v, wanted ErrLOBClosed", when, err)
			}
		}
	}

	t.Run("LobAsReader", func(t *testing.T) {
		rows, err := conn.QueryContext(ctx, qry+tbl+" ORDER BY id", godror.LobAsReader(), godror.FetchArraySize(10))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var prev []*godror.Lob
		var hijacked *godror.DirectLob
		var i int
		for ; rows.Next(); i++ {
			// The Lobs of the previous row are closed, the unread ones, too.
			wantClosed(t, "after Next", prev...)
			if hijacked != nil {
				if _, err := hijacked.ReadAt(make([]byte, 8), 0); err == nil {
					t.Error("ReadAt on the DirectLob of the previous row succeeded")
				}
			}
			var c, b, c2 *godror.Lob
			if err = rows.Scan(&c, &b, &c2); err != nil {
				t.Fatal(err)
			}
			prev = []*godror.Lob{c, b, c2}
			if i%5 == 4 { // leave them unread
				continue
			}
			if got, err := io.ReadAll(c); err != nil {
				t.Fatalf("%d. read CLOB: %+v", i, err)
			} else if string(got) != text(i) {
				t.Errorf("%d. got %d bytes of CLOB, wanted %d", i, len(got), len(text(i)))
			}
			if n, err := b.Size(); err != nil || n != int64(len(text(i))) {
				t.Errorf("%d. BLOB size: got %d, %v; wanted %d", i, n, err, len(text(i)))
			}
			if got, err := io.ReadAll(b); err != nil {
				t.Fatalf("%d. read BLOB: %+v", i, err)
			} else if !bytes.Equal(got, []byte(text(i))) {
				t.Errorf("%d. got %d bytes of BLOB, wanted %d", i, len(got), len(text(i)))
			}
			if i%5 == 3 { // hijack the unread one
				if hijacked, err = c2.Hijack(); err != nil {
					t.Fatal(err)
				}
				if n, err := hijacked.Size(); err != nil || n != int64(len("second-")+len(strconv.Itoa(i))) {
					t.Errorf("%d. DirectLob.Size: got %d, %v", i, n, err)
				}
				prev = prev[:2]
			}
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		if i != N {
			t.Errorf("got %d rows, wanted %d", i, N)
		}
		wantClosed(t, "after the last Next", prev...)
		rows.Close()
		wantClosed(t, "after Close", prev...)
	})

	t.Run("Close", func(t *testing.T) {
		rows, err := conn.QueryContext(ctx, qry+tbl+" ORDER BY id", godror.LobAsReader())
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatal(rows.Err())
		}
		var c, b, c2 *godror.Lob
		if err = rows.Scan(&c, &b, &c2); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		wantClosed(t, "after Close", c, b, c2)
	})

	t.Run("QueryRow", func(t *testing.T) {
		// sql.Row.Scan closes the rows before it returns.
		var c, b, c2 *godror.Lob
		if err := conn.QueryRowContext(ctx, qry+tbl+" WHERE id = 1", godror.LobAsReader()).Scan(&c, &b, &c2); err != nil {
			t.Fatal(err)
		}
		wantClosed(t, "after QueryRow", c, b, c2)
	})

	t.Run("default", func(t *testing.T) {
		// Without LobAsReader the LOBs are read in Next, so they can be kept.
		rows, err := conn.QueryContext(ctx, qry+tbl+" ORDER BY id", godror.FetchArraySize(10))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var cs, c2s []string
		var bs [][]byte
		for rows.Next() {
			var c, c2 string
			var b []byte
			if err = rows.Scan(&c, &b, &c2); err != nil {
				t.Fatal(err)
			}
			cs, bs, c2s = append(cs, c), append(bs, b), append(c2s, c2)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(cs) != N {
			t.Fatalf("got %d rows, wanted %d", len(cs), N)
		}
		for i := range cs {
			if cs[i] != text(i) || string(bs[i]) != text(i) || c2s[i] != "second-"+strconv.Itoa(i) {
				t.Errorf("%d. got %d, %d bytes and %q", i, len(cs[i]), len(bs[i]), c2s[i])
			}
		}
	})
}

// TestLobOutLifetime checks that a Lob returned as an OUT parameter is
// readable till the statement is executed again or closed,
// and returns ErrLOBClosed after that.
func TestLobOutLifetime(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, "BEGIN :1 := TO_CLOB('value-'||:2); END;")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var a [8]byte
	lobs := make([]godror.Lob, 2)
	for i := range lobs {
		lobs[i].IsClob = true
		if _, err = stmt.ExecContext(ctx, sql.Out{Dest: &lobs[i]}, i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := lobs[0].Read(a[:]); !errors.Is(err, godror.ErrLOBClosed) {
		t.Errorf("Read after the next execution: got %v, wanted ErrLOBClosed", err)
	}
	// The Lob of the last execution, and the DirectLob hijacked from it.
	last := lobs[1]
	dl, err := lobs[1].Hijack()
	if err != nil {
		t.Fatal(err)
	}
	if n, err := dl.Size(); err != nil || n != int64(len("value-1")) {
		t.Errorf("DirectLob.Size: got %d, %v", n, err)
	}
	stmt.Close()
	if _, err := last.Read(a[:]); !errors.Is(err, godror.ErrLOBClosed) {
		t.Errorf("Read after the statement is closed: got %v, wanted ErrLOBClosed", err)
	}
	if _, err := dl.ReadAt(a[:], 0); err == nil {
		t.Error("ReadAt on the DirectLob of the closed statement succeeded")
	}
}
