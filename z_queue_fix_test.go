// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func setUpRawQueue(ctx context.Context, t *testing.T, db *sql.DB, qName string) {
	t.Helper()
	qry := `DECLARE
  tbl CONSTANT VARCHAR2(61) := USER||'.` + qName + `_TBL';
  q CONSTANT VARCHAR2(61) := USER||'.` + qName + `';
BEGIN
  BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;
  SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, 'RAW');
  SYS.DBMS_AQADM.CREATE_QUEUE(q, tbl);
  SYS.DBMS_AQADM.start_queue(q);
END;`
	if _, err := db.ExecContext(ctx, qry); err != nil {
		if strings.Contains(err.Error(), "PLS-00201") {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		testDb.ExecContext(context.Background(), `DECLARE
  tbl CONSTANT VARCHAR2(61) := USER||'.`+qName+`_TBL';
  q CONSTANT VARCHAR2(61) := USER||'.`+qName+`';
BEGIN
  BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;
END;`)
	})
}

// TestQueueUseAfterClose checks that Enqueue/Dequeue on a closed Queue
// return an error instead of passing a released handle to ODPI-C.
func TestQueueUseAfterClose(t *testing.T) {
	if testing.Short() {
		t.Skip("skip DBMS_AQ queue test")
	}
	ctx, cancel := testContext(t, 60*time.Second)
	defer cancel()
	const qName = "TEST_QFIX_CLOSED_Q"
	setUpRawQueue(ctx, t, testDb, qName)

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	q, err := godror.NewQueue(ctx, conn, qName, "",
		godror.WithEnqOptions(godror.EnqOptions{Visibility: godror.VisibleImmediate}),
		godror.WithDeqOptions(godror.DeqOptions{Mode: godror.DeqRemove, Visibility: godror.VisibleImmediate, Navigation: godror.NavFirst, Wait: time.Second}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Enqueue([]godror.Message{{Raw: []byte("one")}, {Raw: []byte("two")}}); err != nil {
		t.Fatal(err)
	}
	msgs := make([]godror.Message, 2)
	if n, err := q.Dequeue(msgs); err != nil || n != 2 {
		t.Fatalf("dequeue: n=%d err=%v", n, err)
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	if err = q.Enqueue([]godror.Message{{Raw: []byte("three")}}); err == nil {
		t.Error("Enqueue on a closed queue: wanted error")
	} else {
		t.Log("enqueue:", err)
	}
	if n, err := q.Dequeue(msgs); err == nil {
		t.Errorf("Dequeue on a closed queue: wanted error, got n=%d", n)
	} else {
		t.Log("dequeue:", err)
	}
	// Closing twice is fine.
	if err = q.Close(); err != nil {
		t.Error(err)
	}
}
