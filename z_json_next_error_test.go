// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestKeepJSONNextError checks that a Next that fails on a later column
// releases the JSON it kept for an earlier one (KeepJSON). The caller
// never gets that JSON, and the reference it holds would keep the
// session busy in its pool forever.
func TestKeepJSONNextError(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	// A pool of its own, so that a leaked reference shows in its statistics.
	P.StandaloneConnection = godror.Bool(false)
	P.MinSessions, P.MaxSessions, P.SessionIncrement = 1, 2, 1
	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()
	db.SetMaxIdleConns(2)

	// Column 0 is a JSON, column 1 an INTERVAL DAY TO SECOND that does not
	// fit in a time.Duration: setValue takes a reference for the JSON
	// (KeepJSON), then fails on the interval.
	const qry = `SELECT JSON_OBJECT('n' VALUE 1 RETURNING JSON),
	TO_DSINTERVAL('999999999 23:59:59.999999999') FROM DUAL`
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.QueryContext(ctx, qry, godror.KeepJSON())
	if err != nil {
		var oraErr *godror.OraErr
		if errors.As(err, &oraErr) && (oraErr.Code() == 902 || oraErr.Code() == 40449) {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("Next succeeded")
	}
	if err = rows.Err(); err == nil {
		t.Fatal("rows.Err: got nil, wanted the interval overflow error")
	} else if !strings.Contains(err.Error(), "INTERVAL DAY TO SECOND") {
		t.Fatalf("rows.Err: got %v, wanted the interval overflow error", err)
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}

	// Make database/sql close its connection: the session must go back to
	// the pool, the failed Next must have left no reference behind.
	db.SetMaxIdleConns(0)
	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}

	conn, err = db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var stats godror.PoolStats
	if err = godror.Raw(ctx, conn, func(c godror.Conn) error {
		stats, err = c.GetPoolStats()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if stats.Busy != 1 {
		t.Errorf("the failed Next left its session busy: %s, wanted busy=1", stats)
	}
}
