// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

//go:build go1.27

package godror_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestKeepJSONScanError checks that a Scan that fails on a later column
// releases the JSON it kept for an earlier column (KeepJSON): the caller
// does not close what a failed Scan gave. A reference left behind keeps
// the session busy in the pool.
//
// Before Go 1.27 (no ScanColumn) the driver does not see this error.
func TestKeepJSONScanError(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	// A pool of its own.
	P.StandaloneConnection = godror.Bool(false)
	P.MinSessions, P.MaxSessions, P.SessionIncrement = 1, 2, 1
	db := sql.OpenDB(godror.NewDriver().NewConnector(P))
	defer db.Close()

	rows, err := db.QueryContext(ctx,
		"SELECT JSON_OBJECT('a' VALUE 1 RETURNING JSON), CAST(NULL AS NUMBER) FROM DUAL",
		godror.KeepJSON())
	if err != nil {
		var oraErr *godror.OraErr
		if errors.As(err, &oraErr) && (oraErr.Code() == 902 || oraErr.Code() == 40449) {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	var good, bad godror.JSON
	defer func() { good.Close(); bad.Close() }()
	var null sql.NullInt64
	if err = rows.Scan(&good, &null); err != nil {
		t.Fatal(err)
	}
	var n int
	if err = rows.Scan(&bad, &n); err == nil {
		t.Fatal("scanned NULL into an int")
	}
	rows.Close()
	if _, err = bad.GetValue(godror.JSONOptDefault); !errors.Is(err, godror.ErrJSONClosed) {
		t.Errorf("the JSON of the failed Scan: got %v, wanted ErrJSONClosed", err)
	}

	// The JSON of the Scan that succeeded is the caller's.
	if got, err := good.StringWithOption(godror.JSONOptNumberAsString); err != nil {
		t.Errorf("the JSON of the successful Scan: %v", err)
	} else if want := `{"a":1}`; got != want {
		t.Errorf("got %q, wanted %q", got, want)
	}
	if err = good.Close(); err != nil {
		t.Error(err)
	}

	// The session must be free: make database/sql close its connection,
	// and see what a new one finds in the pool.
	db.SetMaxIdleConns(0)
	db.SetMaxIdleConns(2)
	conn, err := db.Conn(ctx)
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
		t.Errorf("the failed Scan left its session busy: %d sessions are busy, wanted 1", stats.Busy)
	}
}
