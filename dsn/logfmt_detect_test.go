// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package dsn

import (
	"strings"
	"testing"
	"time"
)

// A one-line logfmt DSN without connectString was taken for an old-style
// user/password@connectString, putting the password into the user name.
func TestParseLogfmtWithoutConnectString(t *testing.T) {
	const passw = "secret"
	for _, in := range []string{
		"user=scott password=" + passw + " libDir=/opt/oracle",
		"password=" + passw + " user=scott libDir=/opt/oracle",
		"libDir=/opt/oracle user=scott password=" + passw,
		"standaloneConnection=1 user=scott password=" + passw + " libDir=/opt/oracle",
		"user=scott password=" + passw + "\nlibDir=/opt/oracle",
		"user=scott password=" + passw + " libDir=/opt/oracle connectString=",
	} {
		P, err := Parse(in)
		if err != nil {
			t.Errorf("Parse(%q): %v", in, err)
			continue
		}
		if P.Username != "scott" || P.Password.Secret() != passw || P.ConnectString != "" || P.LibDir != "/opt/oracle" {
			t.Errorf("Parse(%q): user=%q connectString=%q libDir=%q, password is %d bytes",
				in, P.Username, P.ConnectString, P.LibDir, P.Password.Len())
		}
		if s := P.String(); strings.Contains(s, passw) {
			t.Errorf("Parse(%q).String() shows the password: %s", in, s)
		}
	}
}

// Old-style DSNs must be parsed as before.
func TestParseOldStyleWithEqualSign(t *testing.T) {
	const descr = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=db)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=svc)))"
	for _, tC := range []struct {
		in                   string
		user, passw, connStr string
	}{
		{in: "scott/secret@db", user: "scott", passw: "secret", connStr: "db"},
		{in: "user/password@db", user: "user", passw: "password", connStr: "db"},
		{in: "user/pass=word@db", user: "user", passw: "pass=word", connStr: "db"},
		{in: "scott/user=x@db", user: "scott", passw: "user=x", connStr: "db"},
		{in: "sc=ott/secret@db", user: "sc=ott", passw: "secret", connStr: "db"},
		{in: "user/password=secret", user: "user", passw: "password=secret"},
		{in: "password/user=x@db", user: "password", passw: "user=x", connStr: "db"},
		{in: "USER=x/secret@db", user: "USER=x", passw: "secret", connStr: "db"},
		{in: "user =x/secret@db", user: "user =x", passw: "secret", connStr: "db"},
		{in: "scott/secret@" + descr, user: "scott", passw: "secret", connStr: descr},
		{in: descr, connStr: descr},
		{in: "scott@tcps://db:1521/svc?connect_timeout=60&sdu=8128", user: "scott", connStr: "tcps://db:1521/svc?connect_timeout=60&sdu=8128"},
		{in: "user", connStr: "user"},
		{in: "libDir", connStr: "libDir"},
	} {
		P, err := Parse(tC.in)
		if err != nil {
			t.Errorf("Parse(%q): %v", tC.in, err)
			continue
		}
		if P.Username != tC.user || P.Password.Secret() != tC.passw || P.ConnectString != tC.connStr {
			t.Errorf("Parse(%q): user=%q password=%q connectString=%q, wanted %q %q %q",
				tC.in, P.Username, P.Password.Secret(), P.ConnectString, tC.user, tC.passw, tC.connStr)
		}
	}
}

// Every parameter that String writes must be known as a parameter name,
// and Parse must not take an unknown name for a parameter.
func TestParamNames(t *testing.T) {
	var P ConnectionParams
	P.Username, P.ConnectString, P.Timezone = "u", "db", time.UTC
	P.Password, P.NewPassword = NewPassword("p"), NewPassword("n")
	P.Token, P.PrivateKey, P.ConfigDir, P.LibDir, P.Charset = "t", "k", "c", "l", "cs"
	P.OnInitStmts, P.AlterSession = []string{"x"}, [][2]string{{"a", "b"}}
	P.StmtCacheSize, P.ConnClass, P.AdminRole = 1, "cc", SysDBA
	P.EnableEvents, P.NoTZCheck, P.PerSessionTimezone, P.IsPrelim = true, true, true, true
	P.InitOnNewConn, P.NoBreakOnContextCancel = true, true
	P.MinSessions, P.MaxSessions, P.MaxSessionsPerShard, P.SessionIncrement = 1, 2, 1, 1
	P.MaxLifeTime, P.SessionTimeout, P.WaitTimeout, P.PingInterval = 1, 1, 1, 1
	P.Heterogeneous, P.ExternalAuth, P.NoWait, P.StandaloneConnection = Bool(true), Bool(true), Bool(true), Bool(true)
	for _, kv := range strings.Fields(P.StringWithPassword()) {
		if name, _, ok := strings.Cut(kv, "="); ok && !startsWithParam(name+"=") {
			t.Errorf("%q is not in paramNames", name)
		}
	}
	for _, in := range []string{"foo=bar", "User=x", "=x", "x", ""} {
		if startsWithParam(in) {
			t.Errorf("startsWithParam(%q)", in)
		}
	}
}
