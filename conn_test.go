// Copyright 2017 Tamás Gulácsi
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package goracle

import (
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	errors "golang.org/x/xerrors"
)

func TestParseConnString(t *testing.T) {
	wantAt := ConnectionParams{
		Username:       "cc",
		Password:       "c@c*1",
		SID:            "192.168.1.1/cc",
		MaxLifeTime:    DefaultMaxLifeTime,
		SessionTimeout: DefaultSessionTimeout,
		WaitTimeout:    DefaultWaitTimeout,
	}
	wantDefault := ConnectionParams{
		Username:       "user",
		Password:       "pass",
		SID:            "sid",
		ConnClass:      DefaultConnectionClass,
		MinSessions:    DefaultPoolMinSessions,
		MaxSessions:    DefaultPoolMaxSessions,
		PoolIncrement:  DefaultPoolIncrement,
		MaxLifeTime:    DefaultMaxLifeTime,
		SessionTimeout: DefaultSessionTimeout,
		WaitTimeout:    DefaultWaitTimeout}

	wantXO := wantDefault
	wantXO.SID = "localhost/sid"

	wantHeterogeneous := wantXO
	wantHeterogeneous.HeterogeneousPool = true

	setP := func(s, p string) string {
		if i := strings.Index(s, ":SECRET-"); i >= 0 {
			if j := strings.Index(s[i:], "@"); j >= 0 {
				return s[:i+1] + p + s[i+j:]
			}
		}
		return s
	}

	for tName, tCase := range map[string]struct {
		In   string
		Want ConnectionParams
	}{
		"simple": {In: "user/pass@sid", Want: wantDefault},
		"full": {In: "oracle://user:pass@sid/?poolMinSessions=3&poolMaxSessions=9&poolIncrement=3&connectionClass=POOLED&sysoper=1&sysdba=0&poolWaitTimeout=200ms&poolSessionMaxLifetime=4000s&poolSessionTimeout=2000s",
			Want: ConnectionParams{Username: "user", Password: "pass", SID: "sid",
				ConnClass: "POOLED", IsSysOper: true,
				MinSessions: 3, MaxSessions: 9, PoolIncrement: 3,
				WaitTimeout: 200 * time.Millisecond, MaxLifeTime: 4000 * time.Second, SessionTimeout: 2000 * time.Second}},

		"@": {
			In:   setP(wantAt.String(), wantAt.Password),
			Want: wantAt},

		"xo":            {In: "oracle://user:pass@localhost/sid", Want: wantXO},
		"heterogeneous": {In: "oracle://user:pass@localhost/sid?heterogeneousPool=1", Want: wantHeterogeneous},

		"ipv6": {
			In: "oracle://[::1]:12345/dbname",
			Want: ConnectionParams{
				SID:         "[::1]:12345/dbname",
				ConnClass:   "GORACLE",
				MinSessions: 1, MaxSessions: 1000, PoolIncrement: 1,
				WaitTimeout: 30 * time.Second, MaxLifeTime: 1 * time.Hour, SessionTimeout: 5 * time.Minute,
			},
		},
	} {
		t.Log(tCase.In)
		P, err := ParseConnString(tCase.In)
		if err != nil {
			t.Errorf("%s: %v", tName, err)
			continue
		}
		if P != tCase.Want {
			t.Errorf("%s: parse of %q got %#v, wanted %#v\n%s", tName, tCase.In, P, tCase.Want, cmp.Diff(tCase.Want, P))
			continue
		}
		s := setP(P.String(), P.Password)
		Q, err := ParseConnString(s)
		if err != nil {
			t.Errorf("%s: parseConnString %v", tName, err)
			continue
		}
		if P != Q {
			t.Errorf("%s: params got %+v, wanted %+v\n%s", tName, P, Q, cmp.Diff(P, Q))
			continue
		}
		if got := setP(Q.String(), Q.Password); s != got {
			t.Errorf("%s: paramString got %q, wanted %q", tName, got, s)
		}
	}
}

func TestMaybeBadConn(t *testing.T) {
	want := driver.ErrBadConn
	if got := maybeBadConn(errors.Errorf("bad: %w", want), nil); got != want {
		t.Errorf("got %v, wanted %v", got, want)
	}
}

func TestCalculateTZ(t *testing.T) {
	for _, tC := range []struct {
		dbTZ, timezone string
		off            int
		err            error
	}{
		{dbTZ: "Europe/Budapest", timezone: "+01:00", off: 3600},
		{dbTZ: "+01:00", off: +3600},
		{off: 1800, err: io.EOF},
		{timezone: "+00:30", off: 1800},
	} {
		prefix := fmt.Sprintf("%q/%q", tC.dbTZ, tC.timezone)
		_, off, err := calculateTZ(tC.dbTZ, tC.timezone)
		t.Log(prefix, off, err)
		if (err == nil) != (tC.err == nil) {
			t.Errorf("ERR %s: wanted %v, got %v", prefix, tC.err, err)
		} else if err == nil && off != tC.off {
			t.Errorf("ERR %s: got %d, wanted %d.", prefix, off, tC.off)
		}
	}
}
func TestParseTZ(t *testing.T) {
	for k, v := range map[string]int{
		"00:00": 0, "+00:00": 0, "-00:00": 0,
		"01:00": 3600, "+01:00": 3600, "-01:01": -3660,
		"+02:03": 7380,
	} {
		i, err := parseTZ(k)
		if err != nil {
			t.Fatal(errors.Errorf("%s: %w", k, err))
		}
		if i != v {
			t.Errorf("%s. got %d, wanted %d.", k, i, v)
		}
	}
}
