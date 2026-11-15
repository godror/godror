// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"bytes"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// An IN OUT string or []byte longer than 32767 bytes must reach the server.
func TestInOutLongValue(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()

	// The statements are reused, to see that a longer value does not send
	// the previous one.
	const qryS = `DECLARE v CLOB; BEGIN v := :1; :2 := NVL(LENGTH(v), -1); :3 := DBMS_LOB.SUBSTR(v, 1, 1); :1 := v; END;`
	stmtS, err := testDb.PrepareContext(ctx, qryS)
	if err != nil {
		t.Fatal(err)
	}
	defer stmtS.Close()
	const qryB = `DECLARE v BLOB; BEGIN v := :1; :2 := NVL(LENGTH(v), -1); :3 := UTL_RAW.CAST_TO_VARCHAR2(DBMS_LOB.SUBSTR(v, 1, 1)); :1 := v; END;`
	stmtB, err := testDb.PrepareContext(ctx, qryB)
	if err != nil {
		t.Fatal(err)
	}
	defer stmtB.Close()

	for i, n := range []int{100, 32767, 32768, 40000, 200} {
		want := strings.Repeat(string(rune('a'+i)), n)

		s := want
		var length int
		var first string
		if _, err := stmtS.ExecContext(ctx, sql.Out{Dest: &s, In: true}, sql.Out{Dest: &length}, sql.Out{Dest: &first}); err != nil {
			t.Fatalf("string %d: %+v", n, err)
		}
		if length != n || first != want[:1] {
			t.Errorf("string: sent %d bytes of %q, the server saw %d bytes of %q", n, want[:1], length, first)
		}
		if s != want {
			t.Errorf("string: sent %d bytes, got back %d", n, len(s))
		}

		b := []byte(want)
		length, first = 0, ""
		if _, err := stmtB.ExecContext(ctx, sql.Out{Dest: &b, In: true}, sql.Out{Dest: &length}, sql.Out{Dest: &first}); err != nil {
			t.Fatalf("[]byte %d: %+v", n, err)
		}
		if length != n || first != want[:1] {
			t.Errorf("[]byte: sent %d bytes of %q, the server saw %d bytes of %q", n, want[:1], length, first)
		}
		if !bytes.Equal(b, []byte(want)) {
			t.Errorf("[]byte: sent %d bytes, got back %d", n, len(b))
		}
	}
}
