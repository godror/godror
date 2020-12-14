// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"database/sql/driver"
	"fmt"
	"io"
	"testing"
	"time"
)

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
	for _, tC := range []struct {
		sessionTZ, dbOSTZ string
		off               int
		differs           bool
		err               error
	}{
		{sessionTZ: bdpstName, dbOSTZ: bdpstZone, off: bdpstOff},
		{sessionTZ: "+01:00", dbOSTZ: "", off: +3600},
		{sessionTZ: "", dbOSTZ: "", off: 1800, err: io.EOF},
		{sessionTZ: "", dbOSTZ: "+00:30", off: 1800},
		{sessionTZ: "+02:00", dbOSTZ: "+02:00", off: 7200},
		{sessionTZ: "+00:00", dbOSTZ: "-07:00"},
	} {
		prefix := fmt.Sprintf("%q/%q", tC.sessionTZ, tC.dbOSTZ)
		_, off, err := calculateTZ(tC.sessionTZ, tC.dbOSTZ, tC.differs)
		t.Logf("tz=%s => off=%d error=%+v", prefix, off, err)
		if (err == nil) != (tC.err == nil) {
			t.Errorf("ERR %s: wanted %v, got %v", prefix, tC.err, err)
		} else if err == nil && off != tC.off {
			t.Errorf("ERR %s: got %d, wanted %d.", prefix, off, tC.off)
		}
	}
}
