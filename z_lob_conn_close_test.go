// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"io"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// lobPinPool returns a pool that has a single session, so a session that is
// not returned to it makes the next db.Conn fail after 3s.
func lobPinPool(t *testing.T) *sql.DB {
	t.Helper()
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.StandaloneConnection = godror.Bool(false)
	P.MinSessions, P.MaxSessions, P.SessionIncrement = 0, 1, 1
	P.WaitTimeout = 3 * time.Second
	return sql.OpenDB(godror.NewConnector(P))
}

// closeDriverConn closes cx and makes database/sql close the driver conn too.
func closeDriverConn(db *sql.DB, cx *sql.Conn) {
	cx.Close()
	db.SetMaxIdleConns(0)
	db.SetMaxIdleConns(2)
}

// TestUnreadLobDoesNotPinSession checks that LOBs which are kept but never
// read or closed (from LobAsReader rows, and a hijacked DirectLob) do not
// keep a pooled session from being returned when their connection is
// closed, and that reading them afterwards gives an error.
func TestUnreadLobDoesNotPinSession(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	db := lobPinPool(t)
	defer db.Close()

	var kept []*godror.Lob
	for i := 0; i < 3; i++ {
		cx, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("round %d: Conn: %v", i, err)
		}
		rows, err := cx.QueryContext(ctx, "SELECT TO_CLOB(LEVEL) FROM DUAL CONNECT BY LEVEL <= 5", godror.LobAsReader())
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var l *godror.Lob
			if err := rows.Scan(&l); err != nil {
				t.Fatal(err)
			}
			kept = append(kept, l)
		}
		rows.Close()

		var l *godror.Lob
		if err := cx.QueryRowContext(ctx, "SELECT TO_CLOB('hijack') FROM DUAL", godror.LobAsReader()).Scan(&l); err != nil {
			t.Fatal(err)
		}
		if _, err := l.Hijack(); err != nil {
			t.Fatal(err)
		}
		closeDriverConn(db, cx)
	}
	for i, l := range kept {
		if b, err := io.ReadAll(l); err == nil {
			t.Errorf("%d: read %q after the connection was closed, wanted an error", i, b)
		}
	}
}

// TestUnreadObjectLobDoesNotPinSession is the same for a LOB object
// attribute, kept in a Data that is never reset and as a Lob from GetLob.
func TestUnreadObjectLobDoesNotPinSession(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	typName := "TEST_LOBPIN_OT" + tblSuffix
	if _, err := testDb.ExecContext(ctx, "CREATE OR REPLACE TYPE "+typName+" AS OBJECT (id NUMBER, value CLOB)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TYPE "+typName)
	db := lobPinPool(t)
	defer db.Close()

	var kept []*godror.Lob
	var keptData []*godror.Data
	for i := 0; i < 3; i++ {
		cx, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("round %d: Conn: %v", i, err)
		}
		var v any
		if err := cx.QueryRowContext(ctx, "SELECT "+typName+"(1, TO_CLOB('attr')) FROM DUAL").Scan(&v); err != nil {
			t.Fatal(err)
		}
		obj := v.(*godror.Object)
		data := new(godror.Data)
		if err := obj.GetAttribute(data, "VALUE"); err != nil {
			t.Fatal(err)
		}
		kept = append(kept, data.GetLob())
		keptData = append(keptData, data)
		obj.Close()
		closeDriverConn(db, cx)
	}
	for i, l := range kept {
		if b, err := io.ReadAll(l); err == nil {
			t.Errorf("%d: read %q after the connection was closed, wanted an error", i, b)
		}
	}
	for i, d := range keptData {
		if l := d.GetLob(); l != nil {
			t.Errorf("%d: GetLob after the connection was closed returned %v, wanted nil", i, l)
		}
	}
}
