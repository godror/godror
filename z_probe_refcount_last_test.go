package godror

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"
)

// TestProbeDetachedAcquireKeepsPool documents the lifetime of a pool with an
// acquisition abandoned by a cancelled context: the pool must not be released
// while a connection of its connector still uses it, and everything must
// settle (pool gone, driver closed) after the abandoned acquisition finishes.
func TestProbeDetachedAcquireKeepsPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	P := lifetimeTestParams(t, 281*time.Second)
	P.MinSessions, P.MaxSessions, P.WaitTimeout = 1, 1, 30*time.Second
	d := NewDriver()
	defer d.Close()
	c := d.NewConnector(P)
	db := sql.OpenDB(c)
	db.SetMaxIdleConns(0)
	defer db.Close()

	var releaseAttempts atomic.Int32
	old := closePool
	closePool = func(p *connPool) error {
		releaseAttempts.Add(1)
		return old(p)
	}
	defer func() { closePool = old }()

	holder, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()

	waitCtx, waitCancel := context.WithCancel(ctx)
	started := make(chan struct{})
	go func() {
		close(started)
		conn, err := db.Conn(waitCtx)
		if conn != nil {
			_ = conn.Close()
		}
		_ = err
	}()
	<-started
	time.Sleep(300 * time.Millisecond) // the acquisition is waiting for a session now
	waitCancel()

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := releaseAttempts.Load(); got != 0 {
		t.Fatalf("pool release attempted %d times while the holder connection still uses it", got)
	}

	// Closing the holder frees the session: the abandoned wait ends, gives the
	// session back, and only then may the pool be released and the driver close.
	if err := holder.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		d.mu.RLock()
		open, conns := d.dpiContext != nil, d.conns
		d.mu.RUnlock()
		pools := len(poolsWithTimeout(d, 280*time.Second, 282*time.Second))
		if pools == 0 && !open && conns == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("still not settled: pools=%d driverOpen=%t d.conns=%d attempts=%d",
				pools, open, conns, releaseAttempts.Load())
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := releaseAttempts.Load(); got != 1 {
		t.Errorf("pool release called %d times, want 1", got)
	}
}

// TestProbeRefCountIdleBlindness shows that an idle pool (all sessions back,
// nothing in flight) sits at the wrapper's own native refCount of 1 — exactly
// the value a pool that nobody will ever use again has. The native counter
// cannot tell "idle between connects" from "not needed anymore"; only the
// Go-side users count can.
func TestProbeRefCountIdleBlindness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	P := lifetimeTestParams(t, 283*time.Second)
	P.MinSessions, P.MaxSessions, P.WaitTimeout = 1, 1, 30*time.Second
	d := NewDriver()
	defer d.Close()
	c := d.NewConnector(P)
	db := sql.OpenDB(c)
	db.SetMaxIdleConns(0)
	defer db.Close()

	// Acquire a session and give it back: nothing is in flight any more.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	pools := poolsWithTimeout(d, 282*time.Second, 284*time.Second)
	if len(pools) != 1 {
		t.Fatalf("wanted 1 pool, got %d", len(pools))
	}
	d.mu.RLock()
	users := pools[0].users
	d.mu.RUnlock()
	rc := pools[0].dpiPool.refCount
	t.Logf("idle pool: pool.users=%d native refCount=%d", users, rc)
	if rc != 1 {
		t.Errorf("native refCount=%d, want 1 (the wrapper's own reference) when idle", rc)
	}
	if users != 1 {
		t.Errorf("pool.users=%d, want 1 (the connector's persistent use)", users)
	}
	// The connector is still open: the pool must stay despite refCount==1.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(poolsWithTimeout(d, 282*time.Second, 284*time.Second)) != 0 {
		if time.Now().After(deadline) {
			t.Error("pool still present after the connector and its connections are closed")
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// The native refCount cannot decide when a pool is still needed.
// Two sql.DBs share a pool; both run connectionless between queries
// (SetMaxIdleConns(0)), so the pool sits at the wrapper's own refCount of 1
// while the second DB still needs it. Closing the first DB must not release it.
func TestProbeRefCountLastVsUsers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	P := lifetimeTestParams(t, 285*time.Second)
	d := NewDriver()
	defer d.Close()
	db1, db2 := sql.OpenDB(d.NewConnector(P)), sql.OpenDB(d.NewConnector(P))
	db1.SetMaxIdleConns(0)
	db2.SetMaxIdleConns(0)
	defer db1.Close()
	defer db2.Close()
	for _, db := range []*sql.DB{db1, db2} {
		if err := db.PingContext(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Let both pings settle: all sessions are back in the pool, nothing is
	// in flight, so the native refCount is the wrapper's own 1.
	deadline := time.Now().Add(5 * time.Second)
	var pool *connPool
	for {
		pools := poolsWithTimeout(d, 284*time.Second, 286*time.Second)
		if len(pools) == 1 && pools[0].dpiPool != nil && pools[0].dpiPool.refCount == 1 {
			pool = pools[0]
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pool did not settle to idle: %v", pools)
		}
		time.Sleep(50 * time.Millisecond)
	}
	d.mu.RLock()
	users := pool.users
	d.mu.RUnlock()
	t.Logf("idle, both DBs alive: pool.users=%d native refCount=%d", users, pool.dpiPool.refCount)
	if users != 2 {
		t.Errorf("pool.users=%d, want 2 (one per connector)", users)
	}

	// Closing db1 releases only its use: the pool must stay for db2,
	// although the native refCount is 1 (== "wrapper holds last reference").
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	pools := poolsWithTimeout(d, 284*time.Second, 286*time.Second)
	if len(pools) != 1 || pools[0] != pool || pools[0].dpiPool == nil {
		t.Fatalf("closing the first sql.DB harmed the pool of the second: %v", pools)
	}
	var s string
	if err := db2.QueryRowContext(ctx, "SELECT 'a' FROM DUAL").Scan(&s); err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for len(poolsWithTimeout(d, 284*time.Second, 286*time.Second)) != 0 {
		if time.Now().After(deadline) {
			t.Error("pool still present after both sql.DBs are closed")
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}
