// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/godror/godror/dsn"
)

// TestConnectorCloseWaitsForPoolRelease checks that closing the last connector
// does not report completion or close its driver while native release is pending.
func TestConnectorCloseWaitsForPoolRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		calls := 0
		old := closePool
		closePool = func(*connPool) error { calls++; <-release; return nil }
		defer func() { closePool = old }()
		pool := &connPool{key: "slow", users: 1}
		d := &drv{pools: map[string]*connPool{pool.key: pool}, connectors: 1}
		c := connector{drv: d, uses: &connectorUses{pools: map[*connPool]struct{}{pool: {}}, active: true}}
		done := make(chan error, 1)
		go func() { done <- c.Close() }()
		synctest.Wait()
		time.Sleep(2 * driverCloseTimeout)
		synctest.Wait()
		returned := false
		select {
		case err := <-done:
			returned = true
			t.Errorf("Close returned before pool release: %v", err)
		default:
		}
		d.mu.RLock()
		closed := d.pools == nil
		d.mu.RUnlock()
		if closed {
			t.Error("driver closed while pool release was pending")
		}
		close(release)
		synctest.Wait()
		if !returned {
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			default:
				t.Error("Close did not finish after pool release")
			}
		}
		if calls != 1 {
			t.Errorf("pool release called %d times, want 1", calls)
		}
	})
}

// TestConcurrentConnectorCloseKeepsDriverDuringPoolRelease checks that a
// second Close cannot destroy the driver while another connector releases a pool.
func TestConcurrentConnectorCloseKeepsDriverDuringPoolRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		old := closePool
		closePool = func(*connPool) error { <-release; return nil }
		defer func() { closePool = old }()
		pool := &connPool{key: "slow", users: 1}
		d := &drv{pools: map[string]*connPool{pool.key: pool}, connectors: 2}
		c1 := connector{drv: d, uses: &connectorUses{pools: map[*connPool]struct{}{pool: {}}, active: true}}
		c2 := connector{drv: d, uses: &connectorUses{active: true}}
		done := make(chan error, 1)
		go func() { done <- c1.Close() }()
		synctest.Wait()
		if err := c2.Close(); err != nil {
			t.Error(err)
		}
		d.mu.RLock()
		closed := d.pools == nil
		d.mu.RUnlock()
		if closed {
			t.Error("second connector closed driver while pool release was pending")
		}
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
		if d.pools != nil {
			t.Error("driver still open after last connector completed release")
		}
	})
}

// TestConnectorPoolReleaseOwnership checks the shared and held-connection
// reference counts without requiring an Oracle client or database.
func TestConnectorPoolReleaseOwnership(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(fmt.Sprintf("held=%t", held), func(t *testing.T) {
			calls := 0
			old := closePool
			closePool = func(*connPool) error { calls++; return nil }
			defer func() { closePool = old }()
			pool := &connPool{key: "shared", users: 2}
			d := &drv{pools: map[string]*connPool{pool.key: pool}, connectors: 2}
			c1 := connector{drv: d, uses: &connectorUses{pools: map[*connPool]struct{}{pool: {}}, active: true}}
			c2 := connector{drv: d, uses: &connectorUses{pools: map[*connPool]struct{}{pool: {}}, active: true}}
			if held {
				c2.uses.conns = 1
				d.conns = 1
			}
			if err := c1.Close(); err != nil {
				t.Fatal(err)
			}
			if calls != 0 {
				t.Fatal("pool released while second connector still uses it")
			}
			if err := c1.Close(); err != nil {
				t.Fatal(err)
			}
			if err := c2.Close(); err != nil {
				t.Fatal(err)
			}
			if held {
				if calls != 0 || d.pools == nil {
					t.Fatal("pool or driver closed before held connection")
				}
				d.connectorDone(c2.uses, false)
				d.connDone()
			}
			if calls != 1 {
				t.Errorf("pool release called %d times, want 1", calls)
			}
			if d.pools != nil {
				t.Error("driver still open after last reference released")
			}
		})
	}
}

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

// TestSharedDriverClose checks that closing a sql.DB does not close
// the Driver under another sql.DB, or under a connection still in use.
func TestSharedDriverClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	P := lifetimeTestParams(t, 221*time.Second)
	open := func(d *drv) bool {
		d.mu.RLock()
		defer d.mu.RUnlock()
		return d.dpiContext != nil
	}

	for _, tc := range []struct {
		Name                  string
		Standalone, SameConnr bool
	}{
		{Name: "pooled"},
		{Name: "standalone", Standalone: true},
		{Name: "same connector", SameConnr: true},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			P := P
			P.StandaloneConnection = Bool(tc.Standalone)
			d := NewDriver()
			defer d.Close()
			c1, c2 := d.NewConnector(P), d.NewConnector(P)
			if tc.SameConnr {
				c2 = c1
			}
			db1, db2 := sql.OpenDB(c1), sql.OpenDB(c2)
			defer db1.Close()
			defer db2.Close()
			for _, db := range []*sql.DB{db1, db2} {
				if err := db.PingContext(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := db1.Close(); err != nil {
				t.Fatal(err)
			}
			// Do not go on with a closed driver: db2 would use freed memory.
			// (Sharing the connector, db2 is known only by its connections.)
			if !open(d) && !(tc.SameConnr && db2.Stats().OpenConnections == 0) {
				t.Fatal("closing the first sql.DB closed the Driver of the second")
			}
			var s string
			if err := db2.QueryRowContext(ctx, "SELECT 'a' FROM DUAL").Scan(&s); err != nil {
				t.Fatal(err)
			}

			// A connection (with a transaction) still in use when its sql.DB is closed.
			tx, err := db2.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := db2.Close(); err != nil {
				t.Fatal(err)
			}
			if !open(d) {
				t.Fatal("closing the sql.DB closed the Driver of its open transaction")
			}
			if err := tx.QueryRowContext(ctx, "SELECT 'b' FROM DUAL").Scan(&s); err != nil {
				t.Error(err)
			}
			if err := tx.Commit(); err != nil {
				t.Error(err)
			}
			if open(d) {
				t.Error("the Driver is still open after its last sql.DB and connection are closed")
			}
		})
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

// TestSharedConnectorPool checks that two sql.DBs opened with the same
// connector value share one pool till both are closed.
func TestSharedConnectorPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const lo = 241 * time.Second
	c := NewConnector(lifetimeTestParams(t, lo))
	db1, db2 := sql.OpenDB(c), sql.OpenDB(c)
	defer db1.Close()
	defer db2.Close()
	if err := db1.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	conn, err := db2.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	pools := poolsWithTimeout(defaultDrv, lo, lo+1)
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}
	if got := poolsWithTimeout(defaultDrv, lo, lo+1); len(got) != 1 || got[0] != pools[0] {
		t.Fatalf("closing one sql.DB closed the pool of the other, which shares its connector: %v", got)
	}
	conn.Close()
	if conn, err = db2.Conn(ctx); err != nil {
		t.Fatal(err)
	}
	conn2, err := db2.Conn(ctx) // a new session
	if err != nil {
		t.Fatal(err)
	}
	if got := poolsWithTimeout(defaultDrv, lo, lo+1); len(got) != 1 || got[0] != pools[0] {
		t.Errorf("the remaining sql.DB does not use the pool it had: %v, wanted %v", got, pools)
	}
	conn.Close()
	conn2.Close()
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	if got := poolsWithTimeout(defaultDrv, lo, lo+1); len(got) != 0 {
		t.Errorf("both sql.DBs are closed, %d session pools remain", len(got))
	}
}

// TestPoolClosedAfterLateConnect checks that a Connect that ends after
// its connector was closed (database/sql may have one running when
// sql.DB.Close closes the connector) does not leave a pool behind.
func TestPoolClosedAfterLateConnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const lo = 251 * time.Second
	c := NewConnector(lifetimeTestParams(t, lo))
	db := sql.OpenDB(c)
	err := db.PingContext(ctx)
	if closeErr := db.Close(); err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	conn, err := c.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Error(err)
	}
	if got := poolsWithTimeout(defaultDrv, lo, lo+1); len(got) != 0 {
		t.Errorf("the connector and its connection are closed, %d session pools remain", len(got))
	}
}

// TestPoolStatsOfHeldConn checks that a connection held over the Close of
// its sql.DB still sees its pool, while other sql.DBs with the same
// parameters come and go. Run it with -race.
func TestPoolStatsOfHeldConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	P := lifetimeTestParams(t, 261*time.Second)
	db := sql.OpenDB(NewConnector(P))
	defer db.Close()
	held, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := held.Raw(func(dc any) error {
				stats, err := dc.(*conn).GetPoolStats()
				if err == nil && stats.Open == 0 {
					t.Error("the pool of the held connection has no open session:", stats)
					return driver.ErrSkip
				}
				return err
			}); err != nil {
				if err != driver.ErrSkip {
					t.Error(err)
				}
				return
			}
		}
	}()
	if err := db.Close(); err != nil {
		t.Error(err)
	}
	for range 5 {
		db := sql.OpenDB(NewConnector(P))
		if err := db.PingContext(ctx); err != nil {
			t.Error(err)
		}
		db.Close()
	}
	close(stop)
	<-done
}
