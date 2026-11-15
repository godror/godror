// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package dsn

import (
	"testing"
	"time"
)

// Parse(P.StringWithPassword()) must give P back.
func TestStringParseRoundTrip(t *testing.T) {
	P, err := Parse("user=u password=secret connectString=db standaloneConnection=0 poolWaitTimeout=0 poolSessionMaxLifetime=0 poolSessionTimeout=0")
	if err != nil {
		t.Fatal(err)
	}
	if P.WaitTimeout != 0 || P.MaxLifeTime != 0 || P.SessionTimeout != 0 {
		t.Fatalf("parsed %v %v %v, wanted zeros", P.WaitTimeout, P.MaxLifeTime, P.SessionTimeout)
	}
	Q, err := Parse(P.StringWithPassword())
	if err != nil {
		t.Fatal(err)
	}
	if Q.WaitTimeout != 0 || Q.MaxLifeTime != 0 || Q.SessionTimeout != 0 {
		t.Errorf("poolWaitTimeout=0 poolSessionMaxLifetime=0 poolSessionTimeout=0 became %v %v %v",
			Q.WaitTimeout, Q.MaxLifeTime, Q.SessionTimeout)
	}

	now := time.Now()
	instants := []time.Time{now,
		time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(now.Year(), 7, 1, 0, 0, 0, 0, time.UTC),
	}
	locs := []*time.Location{
		time.UTC, time.Local,
		time.FixedZone("CET", 3600),    // not the CET of the tz database
		time.FixedZone("JST", 9*3600),  // not in the tz database
		time.FixedZone("", -5*3600),    // no name
		time.FixedZone("+0530", 19800), // as time.Parse names them
		time.FixedZone("-00:30", -1800),
		time.FixedZone("x", -45*60),
	}
	// names with and without a slash
	for _, name := range []string{"Europe/Budapest", "Japan", "GMT", "EST5EDT", "CET"} {
		if loc, err := time.LoadLocation(name); err != nil {
			t.Logf("skip %s: %v", name, err)
		} else {
			locs = append(locs, loc)
		}
	}
	for _, loc := range locs {
		var P ConnectionParams
		P.Username, P.ConnectString, P.Timezone = "u", "db", loc
		s := P.StringWithPassword()
		Q, err := Parse(s)
		if err != nil {
			t.Errorf("zone %q: Parse(%q): %v", loc, s, err)
			continue
		}
		if Q.Timezone == nil {
			t.Errorf("zone %q: lost (%q)", loc, s)
			continue
		}
		if (loc == time.Local) != (Q.Timezone == time.Local) {
			t.Errorf("zone %q became %q", loc, Q.Timezone)
		}
		for _, ts := range instants {
			_, want := ts.In(loc).Zone()
			if _, got := ts.In(Q.Timezone).Zone(); got != want {
				t.Errorf("zone %q: offset %d became %d at %v (%q)", loc, want, got, ts, s)
			}
		}
	}

	for _, tC := range []struct {
		in   string
		want int
	}{
		{"-00:30", -1800}, {"-0:45", -2700}, {"-00:00", 0}, {"+00:30", 1800}, {"00:30", 1800}, {"-01:30", -5400},
	} {
		if got, err := ParseTZ(tC.in); err != nil {
			t.Errorf("ParseTZ(%s): %v", tC.in, err)
		} else if got != tC.want {
			t.Errorf("ParseTZ(%s) = %d, wanted %d", tC.in, got, tC.want)
		}
	}
}
