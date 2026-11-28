// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// A PooledConn closed twice must not be handed out twice, and the ConnPool
// may be closed while connections are given back to it (run with -race).
func TestConnPoolClose(t *testing.T) {
	ctx, cancel := testContext(t, 60*time.Second)
	defer cancel()

	pool := godror.NewConnPool(testDb, 4)
	c, err := pool.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// "defer c.Close()" and an explicit Close
	if err = c.Close(); err != nil {
		t.Error(err)
	}
	if err = c.Close(); err != nil {
		t.Error(err)
	}
	a, err := pool.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := pool.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if a == b || a.Conn == b.Conn {
		t.Errorf("the pool handed out the same connection twice: %p %p", a.Conn, b.Conn)
	}
	rawA, rawB := a.Conn, b.Conn
	a.Close()
	if err = pool.Close(); err != nil {
		t.Error(err)
	}
	// The pool is closed: it gives no connection, and takes none back.
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Conn on a closed ConnPool panics: %v", r)
			}
		}()
		if _, err = pool.Conn(ctx); !errors.Is(err, sql.ErrConnDone) {
			t.Errorf("Conn on a closed ConnPool: got %v, wanted %v", err, sql.ErrConnDone)
		}
	}()
	if a != b {
		b.Close()
	}
	for _, raw := range []*sql.Conn{rawA, rawB} {
		if err = raw.PingContext(ctx); !errors.Is(err, sql.ErrConnDone) {
			t.Errorf("connection is still open after ConnPool.Close: %v", err)
			raw.Close()
		}
	}

	for i := range 200 {
		pool := godror.NewConnPool(testDb, 2)
		c1, err := pool.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		c2, err := pool.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		raw1, raw2 := c1.Conn, c2.Conn
		var wg sync.WaitGroup
		start := make(chan struct{})
		for _, f := range []func() error{c1.Close, c2.Close, pool.Close} {
			wg.Go(func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("iteration %d: panic: %v", i, r)
					}
				}()
				<-start
				_ = f()
			})
		}
		close(start)
		wg.Wait()
		// One of the Closes may have put a connection into the pool
		// after the pool's Close has drained it.
		pool.Close()
		raw1.Close()
		raw2.Close()
	}
}
