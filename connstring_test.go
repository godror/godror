// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestParseConnString(t *testing.T) {
	t.Parallel()
	cc := DefaultConnectionClass
	if DefaultStandaloneConnection {
		cc = ""
	}
	wantAt := ConnectionParams{
		CommonParams: CommonParams{
			Username: "cc",
			Password: Password{"c@c*1"},
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
		StandaloneConnection: DefaultStandaloneConnection,
		CommonParams: CommonParams{
			Username: "user",
			Password: Password{"pass"},
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
	wantDefault.ConnClass = DefaultConnectionClass
	if wantDefault.StandaloneConnection {
		wantDefault.ConnClass = ""
	}

	wantXO := wantDefault
	wantXO.DSN = "localhost/sid"

	wantHeterogeneous := wantXO
	wantHeterogeneous.Heterogeneous = true
	wantHeterogeneous.StandaloneConnection = false
	wantHeterogeneous.ConnClass = DefaultConnectionClass
	//wantHeterogeneous.PoolParams.Username, wantHeterogeneous.PoolParams.Password = "", ""

	cmpOpts := []cmp.Option{
		cmpopts.IgnoreUnexported(ConnectionParams{}),
		//cmp.Comparer(func(a, b ConnectionParams) bool {
		//return a.String() == b.String()
		//}),
		cmp.Comparer(func(a, b *time.Location) bool {
			if a == b {
				return true
			}
			now := time.Now()
			const tzFmt = "2006-01-02T15:04:05"
			return now.In(a).Format(tzFmt) == now.In(b).Format(tzFmt)
		}),
		cmp.Comparer(func(a, b Password) bool { return a.secret == b.secret }),
	}

	setP := func(s, p string) string {
		if i := strings.Index(s, ":SECRET-"); i >= 0 {
			if j := strings.Index(s[i:], "@"); j >= 0 {
				return s[:i+1] + p + s[i+j:]
			}
		} else if i := strings.Index(s, "=\"SECRET-"); i >= 0 {
			if j := strings.Index(s[i+2:], "\" "); j >= 0 {
				return s[:i+2] + p + s[i+2+j:]
			}
		} else if i := strings.Index(s, "=SECRET-"); i >= 0 {
			if j := strings.Index(s[i+1:], " "); j >= 0 {
				return s[:i+1] + p + s[i+1+j:]
			}
		}
		return s
	}
	// "scott@tcps://salesserver1:1521/sales.us.example.com?ssl_server_cert_dn=\"cn=sales,cn=Oracle Context Server,dc=us,dc=example,dc=com\"&sdu=8128&connect_timeout=60",
	wantEasy := wantDefault
	wantEasy.Username, wantEasy.PoolParams.SessionTimeout = "scott", 42*time.Second
	wantEasy.Password.Reset()
	wantEasy.DSN = "tcps://salesserver1:1521/sales.us.example.com?ssl_server_cert_dn=\"cn=sales,cn=Oracle Context Server,dc=us,dc=example,dc=com\"&sdu=8128&connect_timeout=60"

	for tName, tCase := range map[string]struct {
		In   string
		Want ConnectionParams
	}{
		"simple": {In: "user/pass@sid", Want: wantDefault},
		"full": {In: "oracle://user:pass@sid/?poolMinSessions=3&poolMaxSessions=9&poolIncrement=3&connectionClass=TestClassName&standaloneConnection=0&sysoper=1&sysdba=0&poolWaitTimeout=200ms&poolSessionMaxLifetime=4000s&poolSessionTimeout=2000s",
			Want: ConnectionParams{
				CommonParams: CommonParams{
					Username: "user", Password: Password{"pass"}, DSN: "sid",
					Timezone: time.Local,
				},
				ConnParams: ConnParams{
					ConnClass: "TestClassName", IsSysOper: true,
				},
				PoolParams: PoolParams{
					MinSessions: 3, MaxSessions: 9, SessionIncrement: 3,
					WaitTimeout: 200 * time.Millisecond, MaxLifeTime: 4000 * time.Second, SessionTimeout: 2000 * time.Second,
				},
			},
		},

		"@": {
			In:   setP(wantAt.String(), wantAt.Password.Secret()),
			Want: wantAt,
		},

		"xo":            {In: "oracle://user:pass@localhost/sid", Want: wantXO},
		"heterogeneous": {In: "oracle://user:pass@localhost/sid?heterogeneousPool=1", Want: wantHeterogeneous},

		"ipv6": {
			In: "oracle://[::1]:12345/dbname",
			Want: ConnectionParams{
				StandaloneConnection: DefaultStandaloneConnection,
				CommonParams: CommonParams{
					DSN:      "[::1]:12345/dbname",
					Timezone: time.Local,
				},
				ConnParams: ConnParams{
					ConnClass: cc,
				},
				PoolParams: PoolParams{
					MinSessions: 1, MaxSessions: 1000, SessionIncrement: 1,
					WaitTimeout: 30 * time.Second, MaxLifeTime: 1 * time.Hour, SessionTimeout: 5 * time.Minute,
					ExternalAuth: DefaultStandaloneConnection,
				},
			},
		},

		"easy": {
			In:   "scott@tcps://salesserver1:1521/sales.us.example.com?ssl_server_cert_dn=\"cn=sales,cn=Oracle Context Server,dc=us,dc=example,dc=com\"&sdu=8128&connect_timeout=60\npoolSessionTimeout=42s",
			Want: wantEasy,
		},

		"onInit": {In: "oracle://user:pass@sid/?poolMinSessions=3&poolMaxSessions=9&poolIncrement=3&connectionClass=TestClassName&standaloneConnection=0&sysoper=1&sysdba=0&poolWaitTimeout=200ms&poolSessionMaxLifetime=4000s&poolSessionTimeout=2000s&onInit=a&onInit=b",
			Want: ConnectionParams{
				CommonParams: CommonParams{
					Username: "user", Password: Password{"pass"}, DSN: "sid",
					Timezone: time.Local,
				},
				ConnParams: ConnParams{
					ConnClass: "TestClassName", IsSysOper: true,
				},
				PoolParams: PoolParams{
					MinSessions: 3, MaxSessions: 9, SessionIncrement: 3,
					WaitTimeout: 200 * time.Millisecond, MaxLifeTime: 4000 * time.Second, SessionTimeout: 2000 * time.Second,
				},
				onInitStmts: []string{"a", "b"},
			},
		},
	} {
		tCase := tCase
		t.Run(tName, func(t *testing.T) {
			t.Log(tCase.In)
			P, err := ParseConnString(tCase.In)
			if err != nil {
				t.Errorf("%v", err)
				return
			}
			if diff := cmp.Diff(tCase.Want, P, cmpOpts...); diff != "" && tCase.Want.String() != P.String() {
				t.Errorf("parse of %q\ngot\n\t%#v,\nwanted\n\t%#v\n%s", tCase.In, P, tCase.Want, diff)
				return
			}
			s := setP(P.String(), P.Password.Secret())
			Q, err := ParseConnString(s)
			if err != nil {
				t.Errorf("parseConnString %v", err)
				return
			}
			if diff := cmp.Diff(P, Q, cmpOpts...); diff != "" && P.String() != Q.String() {
				t.Errorf("params got %+v, wanted %+v\n%s", P, Q, diff)
				return
			}
			if got := setP(Q.String(), Q.Password.Secret()); s != got {
				t.Errorf("paramString got %q, wanted %q", got, s)
			}
		})
	}
}
