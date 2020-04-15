// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	errors "golang.org/x/xerrors"
)

func TestParseConnString(t *testing.T) {
	t.Parallel()
	wantAt := ConnectionParams{
		ConnParams: ConnParams{
			UserName: "cc",
			Password: "c@c*1",
			DSN:      "192.168.1.1/cc",
			Timezone: time.Local,
		},
		PoolParams: PoolParams{
			UserName:       "cc",
			Password:       "c@c*1",
			DSN:            "192.168.1.1/cc",
			MaxLifeTime:    DefaultMaxLifeTime,
			SessionTimeout: DefaultSessionTimeout,
			WaitTimeout:    DefaultWaitTimeout,
			Timezone:       time.Local,
		},
	}
	wantDefault := ConnectionParams{
		ConnParams: ConnParams{
			UserName:  "user",
			Password:  "pass",
			DSN:       "sid",
			ConnClass: DefaultConnectionClass,
			Timezone:  time.Local,
		},
		PoolParams: PoolParams{
			UserName:         "user",
			Password:         "pass",
			DSN:              "sid",
			MinSessions:      DefaultPoolMinSessions,
			MaxSessions:      DefaultPoolMaxSessions,
			SessionIncrement: DefaultPoolIncrement,
			MaxLifeTime:      DefaultMaxLifeTime,
			SessionTimeout:   DefaultSessionTimeout,
			WaitTimeout:      DefaultWaitTimeout,
			Timezone:         time.Local,
		},
	}

	wantXO := wantDefault
	wantXO.ConnParams.DSN = "localhost/sid"
	wantXO.PoolParams.DSN = wantXO.ConnParams.DSN

	wantHeterogeneous := wantXO
	wantHeterogeneous.Heterogeneous = true

	setP := func(s, p string) string {
		if i := strings.Index(s, ":SECRET-"); i >= 0 {
			if j := strings.Index(s[i:], "@"); j >= 0 {
				return s[:i+1] + p + s[i+j:]
			}
		}
		return s
	}

	cmpOpts := []cmp.Option{
		cmp.Comparer(func(a *time.Location, b *time.Location) bool {
			var zero time.Time
			const tf = "2006-01-02T15:04:05"
			return a == b ||
				a.String() == b.String() && a.String() != "" ||
				zero.In(a).Format(tf) == zero.In(b).Format(tf)
		}),
	}

	for tName, tCase := range map[string]struct {
		In   string
		Want ConnectionParams
	}{
		"simple": {In: "user/pass@sid", Want: wantDefault},
		"full": {In: "oracle://user:pass@sid/?poolMinSessions=3&poolMaxSessions=9&poolIncrement=3&connectionClass=POOLED&sysoper=1&sysdba=0&poolWaitTimeout=200ms&poolSessionMaxLifetime=4000s&poolSessionTimeout=2000s",
			Want: ConnectionParams{
				ConnParams: ConnParams{
					UserName: "user", Password: "pass", DSN: "sid",
					ConnClass: "POOLED", IsSysOper: true, Timezone: time.Local,
				},
				PoolParams: PoolParams{
					UserName: "user", Password: "pass", DSN: "sid",
					Timezone:    time.Local,
					MinSessions: 3, MaxSessions: 9, SessionIncrement: 3,
					WaitTimeout: 200 * time.Millisecond, MaxLifeTime: 4000 * time.Second, SessionTimeout: 2000 * time.Second,
				},
			},
		},

		"@": {
			In:   setP(wantAt.String(), wantAt.ConnParams.Password),
			Want: wantAt,
		},

		"xo":            {In: "oracle://user:pass@localhost/sid", Want: wantXO},
		"heterogeneous": {In: "oracle://user:pass@localhost/sid?heterogeneousPool=1", Want: wantHeterogeneous},

		"ipv6": {
			In: "oracle://[::1]:12345/dbname",
			Want: ConnectionParams{
				ConnParams: ConnParams{
					DSN:       "[::1]:12345/dbname",
					ConnClass: "GODROR", Timezone: time.Local,
				},
				PoolParams: PoolParams{
					DSN:         "[::1]:12345/dbname",
					Timezone:    time.Local,
					MinSessions: 1, MaxSessions: 1000, SessionIncrement: 1,
					WaitTimeout: 30 * time.Second, MaxLifeTime: 1 * time.Hour, SessionTimeout: 5 * time.Minute,
				},
			},
		},

		"onInit": {In: "oracle://user:pass@sid/?poolMinSessions=3&poolMaxSessions=9&poolIncrement=3&connectionClass=POOLED&sysoper=1&sysdba=0&poolWaitTimeout=200ms&poolSessionMaxLifetime=4000s&poolSessionTimeout=2000s&onInit=a&onInit=b",
			Want: ConnectionParams{
				ConnParams: ConnParams{
					UserName: "user", Password: "pass", DSN: "sid",
					ConnClass: "POOLED", IsSysOper: true, Timezone: time.Local,
					OnInit: []string{"a", "b"},
				},
				PoolParams: PoolParams{
					UserName: "user", Password: "pass", DSN: "sid",
					Timezone:    time.Local,
					MinSessions: 3, MaxSessions: 9, SessionIncrement: 3,
					WaitTimeout: 200 * time.Millisecond, MaxLifeTime: 4000 * time.Second, SessionTimeout: 2000 * time.Second,
				},
			},
		},
	} {
		t.Log(tCase.In)
		P, err := ParseConnString(tCase.In)
		if err != nil {
			t.Errorf("%s: %v", tName, err)
			continue
		}
		if !reflect.DeepEqual(P, tCase.Want) {
			t.Errorf("%s: parse of %q got %#v, wanted %#v\n%s", tName, tCase.In, P, tCase.Want,
				cmp.Diff(tCase.Want, P, cmpOpts...))
			continue
		}
		s := setP(P.String(), P.ConnParams.Password)
		Q, err := ParseConnString(s)
		if err != nil {
			t.Errorf("%s: parseConnString %v", tName, err)
			continue
		}
		if !reflect.DeepEqual(P, Q) {
			t.Errorf("%s: params got %+v, wanted %+v\n%s", tName, P, Q, cmp.Diff(P, Q, cmpOpts...))
			continue
		}
		if got := setP(Q.String(), Q.ConnParams.Password); s != got {
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
