// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestTraceTagPooledSession checks that the trace tag set by one connection
// is not seen by the next connection that gets the same pooled session.
func TestTraceTagPooledSession(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.StandaloneConnection = godror.Bool(false)
	P.MinSessions, P.MaxSessions, P.SessionIncrement = 1, 1, 1
	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()
	// Release the session to the pool after each use.
	db.SetMaxIdleConns(0)

	type tags struct{ SID, Module, Action, Info string }
	get := func(ctx context.Context) tags {
		t.Helper()
		var sid string
		var m, a, i sql.NullString
		const qry = `SELECT SYS_CONTEXT('USERENV', 'SID'), SYS_CONTEXT('USERENV', 'MODULE'),
			SYS_CONTEXT('USERENV', 'ACTION'), SYS_CONTEXT('USERENV', 'CLIENT_INFO') FROM DUAL`
		if err := db.QueryRowContext(ctx, qry).Scan(&sid, &m, &a, &i); err != nil {
			t.Fatal(err)
		}
		return tags{SID: sid, Module: m.String, Action: a.String, Info: i.String}
	}

	first := get(godror.ContextWithTraceTag(ctx, godror.TraceTag{Module: "ttMod", Action: "ttAct", ClientInfo: "ttInfo", DbOp: "ttOp"}))
	if want := (tags{SID: first.SID, Module: "ttMod", Action: "ttAct", Info: "ttInfo"}); first != want {
		t.Fatalf("got %+v, wanted %+v", first, want)
	}

	second := get(godror.ContextWithTraceTag(ctx, godror.TraceTag{Module: "ttMod2"}))
	if second.SID != first.SID {
		t.Skipf("got another session (%s, then %s)", first.SID, second.SID)
	}
	if want := (tags{SID: first.SID, Module: "ttMod2"}); second != want {
		t.Errorf("only Module is set: got %+v, wanted %+v", second, want)
	}

	// testContext sets a trace tag, so use a context without one.
	plainCtx, plainCancel := context.WithTimeout(context.Background(), time.Minute)
	defer plainCancel()
	third := get(plainCtx)
	if third.SID != first.SID {
		t.Skipf("got another session (%s, then %s)", first.SID, third.SID)
	}
	if third.Module == "ttMod2" || third.Action == "ttAct" || third.Info == "ttInfo" {
		t.Errorf("no trace tag: got %+v, the tag of a previous connection", third)
	}
}
