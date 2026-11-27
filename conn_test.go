// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"
	"time"
)

// BreakIdleSessionForTest forces the window between a completed native call
// and its owner's notification to the cancellation watcher.
func BreakIdleSessionForTest(dc Conn) {
	c := dc.(*conn)
	c.mu.RLock()
	defer c.mu.RUnlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	wait := breakOnCancel(ctx, c, done, nil)
	// Keep done open until the watcher has sent BREAK to the idle session.
	if !wait(nil) {
		c.brokenAfterBreak.Store(true)
		c.closeBrokenSession()
	}
	close(done)
}

// The execution has finished locally while BREAK is in flight. A ping can
// itself consume the interrupt; only the following round trip proves reuse.
func TestBreakAfterCompletedCall(t *testing.T) {
	for _, tc := range []struct {
		name         string
		secondPing   error
		wantReusable bool
	}{
		{"pending break consumed", nil, true},
		{"session closed after break", &OraErr{code: 24909}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done, started := make(chan struct{}), make(chan struct{})
			var pending bool
			pings := 0
			wait := watchCancel(ctx, done, func() {
				close(started)
				<-done
				pending = true
			}, func() error {
				pings++
				if pending {
					pending = false
					return &OraErr{code: 1013}
				}
				return tc.secondPing
			})
			cancel()
			<-started
			close(done)
			if reusable := wait(nil); reusable != tc.wantReusable || pings != 2 || pending {
				t.Fatalf("reusable=%t pings=%d pending=%t; want reusable=%t and two pings", reusable, pings, pending, tc.wantReusable)
			}
		})
	}
}

func TestBreakDiscardsUnsynchronizedSession(t *testing.T) {
	c := new(conn)
	c.brokenAfterBreak.Store(true)
	c.closeBrokenSession()
	ctx := context.Background()
	if c.IsValid() || c.ResetSession(ctx) != driver.ErrBadConn {
		t.Fatal("discarded session remained eligible for reuse")
	}
	if _, err := c.PrepareContext(ctx, "SELECT 1 FROM DUAL"); err != driver.ErrBadConn {
		t.Fatalf("PrepareContext: %v", err)
	}
	st := &statement{conn: c}
	if n := st.NumInput(); n != -1 {
		t.Fatalf("NumInput on discarded session returned %d", n)
	}
	if _, err := st.ExecContext(ctx, nil); err != driver.ErrBadConn {
		t.Fatalf("ExecContext: %v", err)
	}
	if _, err := st.QueryContext(ctx, nil); err != driver.ErrBadConn {
		t.Fatalf("QueryContext: %v", err)
	}
}

func TestMaybeBadConn(t *testing.T) {
	t.Parallel()
	want := driver.ErrBadConn
	if got := maybeBadConn(fmt.Errorf("bad: %w", want), nil); got != want {
		t.Errorf("got %v, wanted %v", got, want)
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
