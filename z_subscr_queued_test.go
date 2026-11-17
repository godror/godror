// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"errors"
	"os"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestSubscriptionCloseWithQueuedNotifications closes a client-initiated
// Subscription while notifications keep arriving.
//
// ODPI-C's dpiConn_unsubscribe used to hold subscr->mutex while calling
// OCISubscriptionUnRegister, which (for client-initiated subscriptions) joins
// the OCI notification thread; that thread, delivering the next notification,
// blocks in dpiSubscr__callback on the same mutex: deadlock.
func TestSubscriptionCloseWithQueuedNotifications(t *testing.T) {
	ctx, cancel := testContext(t, 2*time.Minute)
	defer cancel()

	const tbl = "test_subscr_queued"
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (i NUMBER)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl)

	cx, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := godror.DriverConn(ctx, cx)
	if err != nil {
		cx.Close()
		t.Fatal(err)
	}

	var n int32
	cb := func(e godror.Event) {
		atomic.AddInt32(&n, 1)
		time.Sleep(20 * time.Millisecond)
	}
	s, err := conn.NewSubscription("", cb, godror.SubscrClientInitiated(true))
	if err != nil {
		cx.Close()
		var ec interface{ Code() int }
		if errors.As(err, &ec) {
			switch ec.Code() {
			// 24912: server-initiated subscription already used in this process
			case 29970, 65131, 1031, 29972, 24912:
				t.Skip(err.Error())
			}
		}
		t.Fatalf("create %+v", err)
	}
	if err = s.Register("SELECT COUNT(0) FROM " + tbl); err != nil {
		s.Close()
		cx.Close()
		t.Fatalf("%+v", err)
	}

	// Keep notifications coming (and queueing behind the slow callback)
	// until Close has returned.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			testDb.ExecContext(ctx, "INSERT INTO "+tbl+" (i) VALUES (:1)", i)
			time.Sleep(5 * time.Millisecond)
		}
	}()
	defer func() { close(stop); wg.Wait() }()

	deadline := time.Now().Add(10 * time.Second)
	for atomic.LoadInt32(&n) < 5 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&n); got < 5 {
		s.Close()
		cx.Close()
		t.Skipf("only %d notifications delivered", got)
	}

	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Error("close:", err)
		}
		cx.Close()
		t.Logf("closed after %d callbacks", atomic.LoadInt32(&n))
	case <-time.After(30 * time.Second):
		pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		// cx is not closed: its connection is stuck in dpiConn_unsubscribe.
		t.Fatalf("Subscription.Close did not return in 30s (deadlock in dpiConn_unsubscribe?) after %d callbacks", atomic.LoadInt32(&n))
	}
}
