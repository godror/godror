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

// TestQueueEmpty checks that an empty batch and an empty Raw payload
// are handled instead of panicking with index out of range.
func TestQueueEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("skip DBMS_AQ queue test")
	}
	ctx, cancel := testContext(t, 60*time.Second)
	defer cancel()
	qName := strings.ToUpper("TEST_QEMPTY" + tblSuffix)
	setUpRawQueue(ctx, t, testDb, qName)

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	q, err := godror.NewQueue(ctx, conn, qName, "",
		godror.WithEnqOptions(godror.EnqOptions{Visibility: godror.VisibleImmediate, DeliveryMode: godror.DeliverPersistent}),
		godror.WithDeqOptions(godror.DeqOptions{Mode: godror.DeqRemove, Visibility: godror.VisibleImmediate, DeliveryMode: godror.DeliverPersistent, Navigation: godror.NavFirst}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	if err = q.Enqueue(nil); err != nil {
		t.Errorf("Enqueue(nil): %v", err)
	}
	if n, err := q.Dequeue(nil); n != 0 || err != nil {
		t.Errorf("Dequeue(nil): n=%d err=%v", n, err)
	}

	// ODPI-C has no way to send an empty RAW payload: it reports DPI-1070
	// (no payload provided). One message goes through enqOne, two through enqMany.
	for _, in := range [][]godror.Message{
		{{Correlation: "empty"}},
		{{Raw: []byte("a"), Correlation: "a"}, {Raw: []byte{}, Correlation: "empty"}},
	} {
		if err = q.Enqueue(in); err == nil {
			t.Errorf("Enqueue %d messages with an empty Raw: wanted error", len(in))
		} else {
			t.Log(err)
		}
	}
	// Nothing was enqueued, and the queue is still usable.
	if err = q.Enqueue([]godror.Message{{Raw: []byte("b")}}); err != nil {
		t.Fatal(err)
	}
	out := make([]godror.Message, 3)
	if n, err := q.Dequeue(out); err != nil || n != 1 || string(out[0].Raw) != "b" {
		t.Errorf("Dequeue: n=%d err=%v raw=%q, wanted the one message", n, err, out[0].Raw)
	}
}
