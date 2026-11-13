// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestGuardCleanupRuns checks that with GuardWithFinalizers(true) the
// cleanup guards on statement, Object, ObjectType and Queue
// can be registered and actually run when the resource is dropped without Close.
//
// A cleanup whose closure or arg keeps the tracked object reachable never runs
// (and since Go 1.24 AddCleanup panics on the detectable cases).
func TestGuardCleanupRuns(t *testing.T) {
	dsn := os.Getenv("GODROR_TEST_DSN")
	if dsn == "" {
		t.Skip("GODROR_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := sql.Open("godror", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const typName = "TEST_GUARD_CLN_OT"
	const qName = "TEST_GUARD_CLN_Q"
	if _, err := db.ExecContext(ctx, "CREATE OR REPLACE TYPE "+typName+" AS OBJECT (n NUMBER)"); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DROP TYPE "+typName)
	const qDrop = `DECLARE
  q CONSTANT VARCHAR2(128) := USER||'.` + qName + `';
BEGIN
  BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue_table(q||'_TBL', TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;
END;`
	_, _ = db.ExecContext(ctx, qDrop)
	haveQueue := true
	if _, err := db.ExecContext(ctx, `DECLARE
  q CONSTANT VARCHAR2(128) := USER||'.`+qName+`';
BEGIN
  SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(q||'_TBL', 'RAW');
  SYS.DBMS_AQADM.CREATE_QUEUE(q, q||'_TBL');
  SYS.DBMS_AQADM.start_queue(q);
END;`); err != nil {
		t.Logf("cannot create queue (skipping Queue check): %+v", err)
		haveQueue = false
	} else {
		defer db.ExecContext(context.Background(), qDrop)
	}

	cx, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cx.Close()

	// Route all cleanup warnings to stdout (no logger), and capture stdout.
	prevGuard := guardWithFinalizers.Load()
	GuardWithFinalizers(true)
	defer GuardWithFinalizers(prevGuard)
	prevLogger, _ := globalLogger.Load().(*slog.Logger)
	globalLogger.Store((*slog.Logger)(nil))
	defer globalLogger.Store(prevLogger)

	origStdout := os.Stdout
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		defer close(done)
		b := make([]byte, 4096)
		for {
			n, err := pr.Read(b)
			if n > 0 {
				mu.Lock()
				buf.Write(b[:n])
				mu.Unlock()
				origStdout.Write(b[:n])
			}
			if err != nil {
				return
			}
		}
	}()
	os.Stdout = pw
	restore := func() {
		if os.Stdout == pw {
			os.Stdout = origStdout
			pw.Close()
			<-done
		}
	}
	defer restore()

	try := func(name string, f func() error) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("%s: panic: %v", name, r)
			}
		}()
		if err := f(); err != nil {
			t.Errorf("%s: %+v", name, err)
		}
	}

	want := []string{"of prepareContext is not closed", "WARN Object ", "WARN ObjectType "}
	if err := cx.Raw(func(dc any) error {
		c := dc.(*conn)
		// Leak a statement.
		try("statement", func() error {
			_, err := c.PrepareContext(ctx, "SELECT 1 FROM DUAL")
			return err
		})
		// Leak an ObjectType (removed from the conn cache) and an Object of it.
		try("Object", func() error {
			ot, err := c.GetObjectType(typName)
			if err != nil {
				return err
			}
			for k, v := range c.objTypes {
				if v == ot {
					delete(c.objTypes, k)
				}
			}
			_, err = ot.NewObject()
			return err
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if haveQueue {
		want = append(want, "of NewQueue is not Closed")
		try("Queue", func() error {
			_, err := NewQueue(ctx, cx, qName, "")
			return err
		})
	}

	missing := func() []string {
		mu.Lock()
		s := buf.String()
		mu.Unlock()
		var m []string
		for _, w := range want {
			if !strings.Contains(s, w) {
				m = append(m, w)
			}
		}
		return m
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(missing()) != 0 && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}
	restore()
	mu.Lock()
	out := buf.String()
	mu.Unlock()
	if m := missing(); len(m) != 0 {
		t.Errorf("cleanup did not run for %q", m)
	}
	if strings.Contains(out, "error on close") {
		t.Errorf("cleanup release failed: %s", out)
	}
}
