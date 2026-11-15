// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"testing"
	"time"
)

func TestZoneNameOffsets(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	for _, tC := range []struct {
		name string
		off  int
	}{
		{"+05:30", 5*3600 + 30*60},
		{"+11:00", 11 * 3600},
		{"-03:30", -(3*3600 + 30*60)},
		{"-00:30", -30 * 60},
		{"-07:00", -7 * 3600},
		{"-11:00", -11 * 3600},
	} {
		qry := "SELECT TIMESTAMP '2020-06-01 12:00:00 " + tC.name + "' FROM DUAL"
		var tm time.Time
		if err := testDb.QueryRowContext(ctx, qry).Scan(&tm); err != nil {
			t.Errorf("%s: %v", qry, err)
			continue
		}
		if name, off := tm.Zone(); name != tC.name || off != tC.off {
			t.Errorf("%s: got zone %q with offset %d, wanted %q with %d", qry, name, off, tC.name, tC.off)
		}
	}
}
