// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// TestPoolAcquireContext checks that waiting for a session of an exhausted
// pool ends with the context, and that the abandoned wait leaves nothing behind.
func TestPoolAcquireContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	P := lifetimeTestParams(t, 271*time.Second)
	P.MinSessions, P.MaxSessions, P.WaitTimeout = 1, 1, 30*time.Second
	d := NewDriver()
	defer d.Close()
	connector := d.NewConnector(P)
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxIdleConns(0) // closing a *sql.Conn gives the session back to the pool
	state := func() (open bool, conns int) {
		d.mu.RLock()
		defer d.mu.RUnlock()
		return d.dpiContext != nil, d.conns
	}
	// settled waits till the driver has only the given number of connections.
	settled := func(want int) bool {
		for range 100 {
			if _, conns := state(); conns == want {
				return true
			}
			time.Sleep(100 * time.Millisecond)
		}
		return false
	}

	holder, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()

	shortCtx, shortCancel := context.WithTimeout(ctx, time.Second)
	start := time.Now()
	cx, err := db.Conn(shortCtx)
	dur := time.Since(start)
	shortCancel()
	if err == nil {
		cx.Close()
		t.Fatal("got a second connection from a pool of one session")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, wanted %v", err, context.DeadlineExceeded)
	}
	if dur > 5*time.Second {
		t.Errorf("Conn with a context of 1s returned after %s", dur.Round(time.Second))
	}

	cancelledCtx, cancelNow := context.WithCancel(ctx)
	cancelNow()
	if c, err := connector.Connect(cancelledCtx); err == nil {
		c.Close()
		t.Error("Connect with a cancelled context succeeded")
	} else if !errors.Is(err, context.Canceled) {
		t.Errorf("Connect with a cancelled context: got %v, wanted %v", err, context.Canceled)
	}

	// The abandoned wait gets the session when it is free, and gives it back.
	if err := holder.Close(); err != nil {
		t.Fatal(err)
	}
	if !settled(0) {
		_, conns := state()
		t.Errorf("no connection is open, the driver counts %d", conns)
	}
	if cx, err = db.Conn(ctx); err != nil {
		t.Fatal(err)
	}
	defer cx.Close()
	if err := cx.Raw(func(dc any) error {
		stats, err := dc.(*conn).GetPoolStats()
		if err == nil && (stats.Busy != 1 || stats.Open != 1) {
			t.Errorf("one connection is in use, the pool has %s", stats)
		}
		return err
	}); err != nil {
		t.Error(err)
	}
	cx.Close()
	if err := db.Close(); err != nil {
		t.Error(err)
	}
	if open, conns := state(); open || conns != 0 {
		t.Errorf("everything is closed, the driver has open=%t conns=%d", open, conns)
	}
}
