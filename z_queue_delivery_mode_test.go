// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"strings"
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
	qName := strings.ToUpper("TEST_QDLVMODE" + tblSuffix)
	setUpRawQueue(ctx, t, testDb, qName)

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	q, err := godror.NewQueue(ctx, conn, qName, "")
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
}
