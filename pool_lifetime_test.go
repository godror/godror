// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/godror/godror/dsn"
)

// lifetimeTestParams returns pooled connection parameters with the given
// session timeout, which makes the pool key distinct from the other tests'.
func lifetimeTestParams(t *testing.T, sessionTimeout time.Duration) dsn.ConnectionParams {
	s := os.Getenv("GODROR_TEST_DSN")
	if s == "" {
		t.Skip("GODROR_TEST_DSN is not set")
	}
	P, err := dsn.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	P.StandaloneConnection = Bool(false)
	P.MinSessions, P.MaxSessions, P.SessionIncrement = 1, 2, 1
	P.SessionTimeout = sessionTimeout
	return P
}

// poolsWithTimeout returns the pools of d whose session timeout is in [lo, hi).
func poolsWithTimeout(d *drv, lo, hi time.Duration) []*connPool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var pools []*connPool
	for _, p := range d.pools {
		if lo <= p.params.SessionTimeout && p.params.SessionTimeout < hi {
			pools = append(pools, p)
		}
	}
	return pools
}

// TestPoolClosedWithLastDB checks that the session pool of the default driver
// is closed when the last sql.DB that uses it is closed, and not earlier.
func TestPoolClosedWithLastDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const lo, n = 211 * time.Second, 4
	hi := lo + n*time.Second

	// Distinct pool keys, one sql.DB each.
	for i := range n {
		db := sql.OpenDB(NewConnector(lifetimeTestParams(t, lo+time.Duration(i)*time.Second)))
		err := db.PingContext(ctx)
		if closeErr := db.Close(); err != nil || closeErr != nil {
			t.Fatal(err, closeErr)
		}
	}
	if pools := poolsWithTimeout(defaultDrv, lo, hi); len(pools) != 0 {
		t.Errorf("%d sql.DBs were opened and closed, %d session pools remain", n, len(pools))
	}

	// One pool key, two sql.DBs.
	P := lifetimeTestParams(t, lo)
	db1, db2 := sql.OpenDB(NewConnector(P)), sql.OpenDB(NewConnector(P))
	defer db1.Close()
	defer db2.Close()
	for _, db := range []*sql.DB{db1, db2} {
		if err := db.PingContext(ctx); err != nil {
			t.Fatal(err)
		}
	}
	pools := poolsWithTimeout(defaultDrv, lo, hi)
	if len(pools) != 1 {
		t.Fatalf("two sql.DBs with the same parameters use %d pools, wanted 1", len(pools))
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}
	if got := poolsWithTimeout(defaultDrv, lo, hi); len(got) != 1 || got[0] != pools[0] || got[0].dpiPool == nil {
		t.Fatalf("closing one sql.DB closed the pool of the other: %v", got)
	}
	conn, err := db2.Conn(ctx) // a second, new session of the pool
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	if got := poolsWithTimeout(defaultDrv, lo, hi); len(got) != 0 {
		t.Errorf("both sql.DBs are closed, %d session pools remain", len(got))
	}
}

// TestPoolClosedAfterHeldConn checks that a connection still in use when
// its sql.DB is closed remains usable, and that the pool is gone after it.
func TestPoolClosedAfterHeldConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const lo = 231 * time.Second
	db := sql.OpenDB(NewConnector(lifetimeTestParams(t, lo)))
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var s string
	if err := conn.QueryRowContext(ctx, "SELECT 'a' FROM DUAL").Scan(&s); err != nil {
		t.Error(err)
	}
	if err := conn.Close(); err != nil {
		t.Error(err)
	}
	if got := poolsWithTimeout(defaultDrv, lo, lo+1); len(got) != 0 {
		t.Errorf("the sql.DB and its connection are closed, %d session pools remain", len(got))
	}
}
