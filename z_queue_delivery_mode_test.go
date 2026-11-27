// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestQueueOptionsDeliveryMode checks that DeqOptions and EnqOptions
// report the DeliveryMode that was set, not always DeliverPersistent.
func TestQueueOptionsDeliveryMode(t *testing.T) {
	if testing.Short() {
		t.Skip("skip DBMS_AQ queue test")
	}
	ctx, cancel := testContext(t, 60*time.Second)
	defer cancel()
	// Only the client-side queue options are used; no queue table is needed.
	const qName = "TEST_QDLVMODE"

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	q, err := godror.NewQueue(ctx, conn, qName, "", godror.WithQueueTable(qName+"_TBL"))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	for _, want := range []godror.DeliveryMode{
		godror.DeliverPersistent, godror.DeliverBuffered, godror.DeliverPersistentOrBuffered,
	} {
		D := godror.DefaultDeqOptions
		D.DeliveryMode = want
		if err = q.SetDeqOptions(D); err != nil {
			t.Fatal(err)
		}
		if D, err = q.DeqOptions(); err != nil {
			t.Fatal(err)
		} else if D.DeliveryMode != want {
			t.Errorf("DeqOptions().DeliveryMode=%d, was set to %d", D.DeliveryMode, want)
		}

		E := godror.DefaultEnqOptions
		E.DeliveryMode = want
		if err = q.SetEnqOptions(E); err != nil {
			t.Fatal(err)
		}
		if E, err = q.EnqOptions(); err != nil {
			t.Fatal(err)
		} else if E.DeliveryMode != want {
			t.Errorf("EnqOptions().DeliveryMode=%d, was set to %d", E.DeliveryMode, want)
		}
	}

	t.Run("concurrent snapshots", func(t *testing.T) {
		// Visibility comes from ODPI-C and delivery mode from the Go cache.
		// Every read should return a pair written by the same setter call.
		enq := [2]godror.EnqOptions{godror.DefaultEnqOptions, godror.DefaultEnqOptions}
		deq := [2]godror.DeqOptions{godror.DefaultDeqOptions, godror.DefaultDeqOptions}
		enq[1].Visibility, enq[1].DeliveryMode = godror.VisibleImmediate, godror.DeliverBuffered
		deq[1].Visibility, deq[1].DeliveryMode = godror.VisibleImmediate, godror.DeliverBuffered
		if err := q.SetEnqOptions(enq[0]); err != nil {
			t.Fatal(err)
		}
		if err := q.SetDeqOptions(deq[0]); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Go(func() {
			for i := range 100 {
				if err := q.SetEnqOptions(enq[i%2]); err != nil {
					t.Error(err)
					return
				}
				if err := q.SetDeqOptions(deq[i%2]); err != nil {
					t.Error(err)
					return
				}
			}
		})
		defer wg.Wait()
		for range 100 {
			e, err := q.EnqOptions()
			if err != nil {
				t.Fatal(err)
			}
			if e != enq[0] && e != enq[1] {
				t.Fatalf("mixed enqueue options: %+v", e)
			}
			d, err := q.DeqOptions()
			if err != nil {
				t.Fatal(err)
			}
			if !(d.Visibility == deq[0].Visibility && d.DeliveryMode == deq[0].DeliveryMode ||
				d.Visibility == deq[1].Visibility && d.DeliveryMode == deq[1].DeliveryMode) {
				t.Fatalf("mixed dequeue options: %+v", d)
			}
		}
	})
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.EnqOptions(); !errors.Is(err, driver.ErrBadConn) {
		t.Errorf("EnqOptions after Close: got %v, want ErrBadConn", err)
	}
	if _, err := q.DeqOptions(); !errors.Is(err, driver.ErrBadConn) {
		t.Errorf("DeqOptions after Close: got %v, want ErrBadConn", err)
	}
}
