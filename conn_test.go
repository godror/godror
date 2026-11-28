// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/godror/godror/dsn"
)

func TestMaybeBadConn(t *testing.T) {
	t.Parallel()
	want := driver.ErrBadConn
	if got := maybeBadConn(fmt.Errorf("bad: %w", want), nil); got != want {
		t.Errorf("got %v, wanted %v", got, want)
	}
}

// A BREAK may reach the server only after the statement it was meant to
// interrupt has completed. drainBreak consumes it with a round trip, and
// verifies the session with one more ping if the first one was interrupted.
func TestDrainBreak(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		errs         []error
		pings        int
		wantReusable bool
	}{
		{"no pending break", []error{nil}, 1, true},
		{"interrupt consumed by ping", []error{&OraErr{code: 1013}, nil}, 2, true},
		{"interrupt repeated", []error{&OraErr{code: 1013}, &OraErr{code: 1013}}, 2, false},
		{"session gone after interrupt", []error{&OraErr{code: 1013}, &OraErr{code: 24909}}, 2, false},
		{"session gone", []error{&OraErr{code: 24909}}, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pings := 0
			reusable := drainBreak(func() error {
				err := tc.errs[pings]
				pings++
				return err
			})
			if reusable != tc.wantReusable || pings != tc.pings {
				t.Fatalf("reusable=%t pings=%d, want reusable=%t pings=%d", reusable, pings, tc.wantReusable, tc.pings)
			}
		})
	}
}

// TestLateBreakIsDrained checks that a BREAK sent by breakOnCancel after a
// statement in an open transaction has completed is drained by wait without
// losing the transaction, and that an unsynchronized session (deadSession = 1)
// is rejected with driver.ErrBadConn across connection and statement methods.
func TestLateBreakIsDrained(t *testing.T) {
	connString := os.Getenv("GODROR_TEST_DSN")
	if connString == "" {
		connString = dsn.DefaultTestDSN
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := sql.Open("godror", connString)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqlConn, err := db.Conn(ctx)
	if err != nil {
		if os.Getenv("GODROR_TEST_DSN") == "" {
			t.Skipf("default test DB unavailable: %v", err)
		}
		t.Fatal(err)
	}
	defer sqlConn.Close()
	tx, err := sqlConn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SAVEPOINT before_break"); err != nil {
		t.Fatal(err)
	}

	var dc *conn
	if err := sqlConn.Raw(func(x any) error { dc = x.(*conn); return nil }); err != nil {
		t.Fatal(err)
	}

	// The watcher's context is canceled while done is still open, so it sends
	// BREAK to a session on which nothing is executing.
	breakCtx, breakCancel := context.WithCancel(context.Background())
	breakCancel()
	dc.mu.RLock()
	done := make(chan struct{})
	wait := breakOnCancel(breakCtx, dc, done, nil)
	if !wait(nil) {
		dc.mu.RUnlock()
		close(done)
		t.Fatal("wait reported healthy session as unusable")
	}
	dc.mu.RUnlock()
	close(done)

	if _, err := tx.ExecContext(ctx, "ROLLBACK TO before_break"); err != nil {
		t.Fatalf("first call after a late BREAK: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// When drainBreak fails, marking dpiConn.deadSession = 1 must prevent
	// any further reuse of the session or its prepared statements.
	stI, err := dc.PrepareContext(ctx, "SELECT 1 FROM DUAL")
	if err != nil {
		t.Fatal(err)
	}
	defer stI.Close()
	st := stI.(*statement)

	dc.dpiConn.deadSession = 1
	if dc.IsValid() || dc.ResetSession(ctx) != driver.ErrBadConn {
		t.Fatal("dead session remained eligible for reuse")
	}
	if _, err := dc.BeginTx(ctx, driver.TxOptions{}); err != driver.ErrBadConn {
		t.Fatalf("BeginTx: %v", err)
	}
	if n := st.NumInput(); n != -1 {
		t.Fatalf("NumInput on dead session returned %d, want -1", n)
	}
	if _, err := st.ExecContext(ctx, nil); err != driver.ErrBadConn {
		t.Fatalf("ExecContext: %v", err)
	}
	if _, err := st.QueryContext(ctx, nil); err != driver.ErrBadConn {
		t.Fatalf("QueryContext: %v", err)
	}
	if _, err := dc.PrepareContext(ctx, "SELECT 1 FROM DUAL"); err != driver.ErrBadConn {
		t.Fatalf("PrepareContext: %v", err)
	}
}

func TestCalculateTZ(t *testing.T) {
	t.Parallel()
	const bdpstName = "Europe/Budapest"
	bdpstZone, bdpstOff := "+01:00", int(3600)
	bdpstLoc, err := time.LoadLocation(bdpstName)
	if err != nil {
		t.Log(err)
	} else {
		nowUTC := time.Now().UTC()
		const format = "20060102T150405"
		nowBdpst, err := time.ParseInLocation(format, nowUTC.Format(format), bdpstLoc)
		if err != nil {
			t.Fatal(err)
		}
		bdpstOff = int(nowUTC.Sub(nowBdpst) / time.Second)
		secs := bdpstOff % 3600
		if secs < 0 {
			secs = -secs
		}
		bdpstZone = fmt.Sprintf("%+02d:%02d", bdpstOff/3600, secs)
	}
	const Hour = 3600
	for _, tC := range []struct {
		err          error
		dbTZ, dbOSTZ string
		off          int
	}{
		{dbTZ: bdpstName, dbOSTZ: bdpstZone, off: bdpstOff},
		{dbTZ: "+01:00", dbOSTZ: "", off: +Hour},
		{dbTZ: "", dbOSTZ: "", off: 1800, err: io.EOF},
		{dbTZ: "", dbOSTZ: "+00:30", off: +Hour / 2},
		{dbTZ: "+02:00", dbOSTZ: "+02:00", off: +2 * Hour},
		{dbTZ: "+00:00", dbOSTZ: "-07:00", off: 0},
		{dbTZ: "-07:00", dbOSTZ: "+00:00", off: -7 * Hour},
		{dbTZ: "Japan", dbOSTZ: "+00:00", off: 9 * Hour},
	} {
		prefix := fmt.Sprintf("%q/%q", tC.dbTZ, tC.dbOSTZ)
		_, off, err := calculateTZ(tC.dbTZ, tC.dbOSTZ, nil)
		t.Logf("tz=%s => off=%d error=%+v", prefix, off, err)
		if (err == nil) != (tC.err == nil) {
			t.Errorf("ERR %s: wanted %v, got %v", prefix, tC.err, err)
		} else if err == nil && off != tC.off {
			t.Errorf("ERR %s: got %d, wanted %d.", prefix, off, tC.off)
		}
	}
}

func TestTZName(t *testing.T) {
	t.Log("tzNamesLC", tzNamesLC)
	chicago, err := time.LoadLocation("America/Chicago")
	if err != nil {
		t.Fatal(err)
	}
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Fatal(err)
	}
	for in, want := range map[string]struct {
		*time.Location
		error
	}{
		"UTC":              {time.UTC, nil},
		"Europe/Berlin":    {berlin, nil},
		"America/Chicago":  {chicago, nil},
		"AMERICA/CHICAGO":  {chicago, nil},
		"AMERICA/NeW_yORk": {newYork, nil},
	} {
		got, err := findProperTZName(in)
		if err != want.error {
			t.Errorf("%q got error %+v, wanted %v", in, err, want.error)
		} else if got != want.Location && got.String() != want.Location.String() {
			t.Errorf("%q got %v, wanted %v", in, got, want.Location)
		}
	}
}
