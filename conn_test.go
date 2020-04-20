// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	//"github.com/google/go-cmp/cmp/cmpopts"
	errors "golang.org/x/xerrors"
)

func TestParseConnString(t *testing.T) {
	t.Parallel()
	wantAt := ConnectionParams{
		CommonParams: CommonParams{
			Username: "cc",
			Password: "c@c*1",
			DSN:      "192.168.1.1/cc",
			Timezone: time.Local,
		},
		PoolParams: PoolParams{
			MaxLifeTime:    DefaultMaxLifeTime,
			SessionTimeout: DefaultSessionTimeout,
			WaitTimeout:    DefaultWaitTimeout,
		},
	}
	wantDefault := ConnectionParams{
		CommonParams: CommonParams{
			Username: "user",
			Password: "pass",
			DSN:      "sid",
			Timezone: time.Local,
		},
		ConnParams: ConnParams{
			ConnClass: DefaultConnectionClass,
		},
		PoolParams: PoolParams{
			MinSessions:      DefaultPoolMinSessions,
			MaxSessions:      DefaultPoolMaxSessions,
			SessionIncrement: DefaultPoolIncrement,
			MaxLifeTime:      DefaultMaxLifeTime,
			SessionTimeout:   DefaultSessionTimeout,
			WaitTimeout:      DefaultWaitTimeout,
		},
	}

	wantXO := wantDefault
	wantXO.DSN = "localhost/sid"

	wantHeterogeneous := wantXO
	wantHeterogeneous.Heterogeneous = true
	//wantHeterogeneous.PoolParams.Username, wantHeterogeneous.PoolParams.Password = "", ""

	cmpOpts := []cmp.Option{
		//cmpopts.IgnoreUnexported(ConnectionParams{}),
		cmp.Comparer(func(a, b ConnectionParams) bool {
			return a.String() == b.String()
		}),
		cmp.Comparer(func(a, b *time.Location) bool {
			if a == b {
				return true
			}
			now := time.Now()
			const tzFmt = "2006-01-02T15:04:05"
			return now.In(a).Format(tzFmt) == now.In(b).Format(tzFmt)
		}),
	}

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
			Want: ConnectionParams{
				CommonParams: CommonParams{
					Username: "user", Password: "pass", DSN: "sid",
					Timezone: time.Local,
				},
				ConnParams: ConnParams{
					ConnClass: "POOLED", IsSysOper: true,
				},
				PoolParams: PoolParams{
					MinSessions: 3, MaxSessions: 9, SessionIncrement: 3,
					WaitTimeout: 200 * time.Millisecond, MaxLifeTime: 4000 * time.Second, SessionTimeout: 2000 * time.Second,
				},
			},
		},

		"@": {
			In:   setP(wantAt.String(), wantAt.Password),
			Want: wantAt,
		},

		"xo":            {In: "oracle://user:pass@localhost/sid", Want: wantXO},
		"heterogeneous": {In: "oracle://user:pass@localhost/sid?heterogeneousPool=1", Want: wantHeterogeneous},

		"ipv6": {
			In: "oracle://[::1]:12345/dbname",
			Want: ConnectionParams{
				CommonParams: CommonParams{
					DSN:      "[::1]:12345/dbname",
					Timezone: time.Local,
				},
				ConnParams: ConnParams{
					ConnClass: "GODROR",
				},
				PoolParams: PoolParams{
					MinSessions: 1, MaxSessions: 1000, SessionIncrement: 1,
					WaitTimeout: 30 * time.Second, MaxLifeTime: 1 * time.Hour, SessionTimeout: 5 * time.Minute,
				},
			},
		},

		"onInit": {In: "oracle://user:pass@sid/?poolMinSessions=3&poolMaxSessions=9&poolIncrement=3&connectionClass=POOLED&sysoper=1&sysdba=0&poolWaitTimeout=200ms&poolSessionMaxLifetime=4000s&poolSessionTimeout=2000s&onInit=a&onInit=b",
			Want: ConnectionParams{
				CommonParams: CommonParams{
					Username: "user", Password: "pass", DSN: "sid",
					Timezone: time.Local,
				},
				ConnParams: ConnParams{
					ConnClass: "POOLED", IsSysOper: true,
				},
				PoolParams: PoolParams{
					MinSessions: 3, MaxSessions: 9, SessionIncrement: 3,
					WaitTimeout: 200 * time.Millisecond, MaxLifeTime: 4000 * time.Second, SessionTimeout: 2000 * time.Second,
				},
				onInitStmts: []string{"a", "b"},
			},
		},
	} {
		t.Log(tCase.In)
		P, err := ParseConnString(tCase.In)
		if err != nil {
			t.Errorf("%s: %v", tName, err)
			continue
		}
		if diff := cmp.Diff(tCase.Want, P, cmpOpts...); diff != "" {
			t.Errorf("%s: parse of %q got %#v, wanted %#v\n%s", tName, tCase.In, P, tCase.Want, diff)
			continue
		}
		s := setP(P.String(), P.Password)
		Q, err := ParseConnString(s)
		if err != nil {
			t.Errorf("%s: parseConnString %v", tName, err)
			continue
		}
		if diff := cmp.Diff(P, Q, cmpOpts...); diff != "" {
			t.Errorf("%s: params got %+v, wanted %+v\n%s", tName, P, Q, diff)
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
		dbTZ, timezone string
		off            int
		err            error
	}{
		{dbTZ: bdpstName, timezone: bdpstZone, off: bdpstOff},
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
