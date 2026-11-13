// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestSubscriptionCloseDuringCallback closes a Subscription while a
// notification callback is in flight.
//
// Close must not free the callback context, drop the registry entry or clear
// the callback before the subscription is unregistered in ODPI-C (which waits
// for the in-flight callback), otherwise the race detector reports a data
// race on Subscription.callback and a callback delivered in that window
// dereferences freed memory / a nil *Subscription.
//
// Only one notification is produced per round: with more notifications
// queued, ODPI-C's dpiConn_unsubscribe can deadlock against OCI's
// client-initiated notification thread (dpiSubscr__callback waits for
// subscr->mutex, which dpiConn_unsubscribe holds while OCI joins that thread).
func TestSubscriptionCloseDuringCallback(t *testing.T) {
	ctx, cancel := testContext(t, 2*time.Minute)
	defer cancel()

	const tbl = "test_subscr_close"
	testDb.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := testDb.ExecContext(ctx, "CREATE TABLE "+tbl+" (i NUMBER)"); err != nil {
		t.Fatal(err)
	}
	defer testDb.Exec("DROP TABLE " + tbl)

	for round := 0; round < 3; round++ {
		func() {
			cx, err := testDb.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer cx.Close()
			conn, err := godror.DriverConn(ctx, cx)
			if err != nil {
				t.Fatal(err)
			}

			var mu sync.Mutex
			var n int
			cb := func(e godror.Event) {
				// Keep the callback in flight while Close is called.
				time.Sleep(time.Second)
				mu.Lock()
				n++
				mu.Unlock()
			}
			s, err := conn.NewSubscription("", cb, godror.SubscrClientInitiated(true))
			if err != nil {
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
				t.Fatalf("%+v", err)
			}
			if _, err := testDb.ExecContext(ctx, "INSERT INTO "+tbl+" (i) VALUES (:1)", round); err != nil {
				s.Close()
				t.Fatal(err)
			}
			// Do not synchronize with the callback here: the race detector
			// must see Close and the in-flight callback as concurrent.
			time.Sleep(500 * time.Millisecond)
			if err := s.Close(); err != nil {
				t.Log("close:", err)
			}
			time.Sleep(100 * time.Millisecond)
			mu.Lock()
			got := n
			mu.Unlock()
			t.Logf("round %d: %d callbacks", round, got)
			if got == 0 {
				t.Skip("no notification delivered")
			}
		}()
	}
}
