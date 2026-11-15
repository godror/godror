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

// TestQueueSetOptionsKept checks that the options set with SetDeqOptions,
// SetDeqCorrelation and SetEnqOptions are still in effect after a
// DequeueWithOptions or EnqueueWithOptions call with other options.
func TestQueueSetOptionsKept(t *testing.T) {
	if testing.Short() {
		t.Skip("skip DBMS_AQ queue test")
	}
	ctx, cancel := testContext(t, 60*time.Second)
	defer cancel()
	qName := strings.ToUpper("TEST_QOPTS" + tblSuffix)
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

	// Enqueue options.
	immediate := godror.EnqOptions{Visibility: godror.VisibleImmediate, DeliveryMode: godror.DeliverPersistent}
	if err = q.SetEnqOptions(immediate); err != nil {
		t.Fatal(err)
	}
	onCommit := immediate
	onCommit.Visibility = godror.VisibleOnCommit
	if err = q.EnqueueWithOptions([]godror.Message{{Raw: []byte("a"), Correlation: "A"}}, &onCommit); err != nil {
		t.Fatal(err)
	}
	if err = q.Enqueue([]godror.Message{{Raw: []byte("b"), Correlation: "B"}, {Raw: []byte("c"), Correlation: "C"}}); err != nil {
		t.Fatal(err)
	}
	if E, err := q.EnqOptions(); err != nil {
		t.Fatal(err)
	} else if E.Visibility != godror.VisibleImmediate {
		t.Errorf("enqueue visibility is %d after EnqueueWithOptions, SetEnqOptions has set %d", E.Visibility, godror.VisibleImmediate)
	}
	if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}

	// Dequeue options: browse, so the messages stay in the queue.
	browse := godror.DeqOptions{
		Mode: godror.DeqBrowse, Navigation: godror.NavFirst, Correlation: "B",
		Visibility: godror.VisibleOnCommit, DeliveryMode: godror.DeliverPersistent,
	}
	if err = q.SetDeqOptions(browse); err != nil {
		t.Fatal(err)
	}
	browseC := browse
	browseC.Correlation = "C"
	msgs := make([]godror.Message, 1)
	for _, want := range []string{"B", "A"} {
		if n, err := q.DequeueWithOptions(msgs, &browseC); err != nil || n != 1 || msgs[0].Correlation != "C" {
			t.Fatalf("DequeueWithOptions: n=%d err=%v correlation=%q, wanted C", n, err, msgs[0].Correlation)
		}
		n, err := q.Dequeue(msgs)
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 || msgs[0].Correlation != want {
			t.Errorf("Dequeue after DequeueWithOptions: n=%d correlation=%q, wanted %q", n, msgs[0].Correlation, want)
		}
		if D, err := q.DeqOptions(); err != nil {
			t.Fatal(err)
		} else if D.Mode != godror.DeqBrowse {
			t.Errorf("dequeue mode is %d after DequeueWithOptions, SetDeqOptions has set %d", D.Mode, godror.DeqBrowse)
		}
		// For the second round.
		if err = q.SetDeqCorrelation("A"); err != nil {
			t.Fatal(err)
		}
	}
}
