// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package dsn

import (
	"strings"
	"testing"
)

// The old-style user/password@connectString form: a backslash escapes
// @, / and itself, and is kept before a letter or a digit.
func TestParseOldStyleBackslash(t *testing.T) {
	for _, tC := range []struct {
		in                   string
		user, passw, connStr string
	}{
		{`scott/p\\ss@host:1521/svc`, "scott", `p\ss`, "host:1521/svc"},
		{`scott/p\ss@host:1521/svc`, "scott", `p\ss`, "host:1521/svc"},
		{`scott/end\\@host/svc`, "scott", `end\`, "host/svc"},
		{`scott/\\\@x@host/svc`, "scott", `\@x`, "host/svc"},
		{`scott/p\@s\/s\*@host/svc`, "scott", "p@s/s*", "host/svc"},
		{`sc\/ott/a\tb@host/svc`, "sc/ott", "a\tb", "host/svc"},
		{`scott/tiger@host/svc?wallet_location=C:\\wallet`, "scott", "tiger", `host/svc?wallet_location=C:\wallet`},
		{`scott/connectString=x@host/svc`, "scott", "connectString=x", "host/svc"},
		{`scott/xconnectString=@host/svc`, "scott", "xconnectString=", "host/svc"},
		// these are logfmt
		{`user=scott password=p\\ss connectString=host/svc`, "scott", `p\\ss`, "host/svc"},
		{"foo=bar user=scott password=tiger\tconnectString=host/svc", "scott", "tiger", "host/svc"},
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

	// Every printable ASCII character, alone and inside a password, and
	// some others: the escaped old-style form and String must give them back.
	passwords := []string{"pässwörd✓", "ü", `a"b'c d`, `\\`, `\@`, `@\`, `/\/`, `\n`, `a\`, `\ü`, "user=x", "p AS SYSDBA"}
	for c := byte(' ' + 1); c < 0x7f; c++ {
		passwords = append(passwords, string(c), "p"+string(c)+"w", string(c)+string(c), "p"+string(c))
	}
	escape := strings.NewReplacer(`\`, `\\`, "@", `\@`, "/", `\/`).Replace
	for _, passw := range passwords {
		for _, connStr := range []string{"host:1521/svc", "tnsalias", "tcps://host:2484/svc?wallet_location=/opt/w"} {
			var P ConnectionParams
			P.Username, P.ConnectString = "scott", connStr
			P.Password.Set(passw)
			for _, in := range []string{
				"scott/" + escape(passw) + "@" + connStr,
				P.StringWithPassword(),
			} {
				Q, err := Parse(in)
				if err != nil {
					t.Errorf("Parse(%q): %v", in, err)
				} else if Q.Username != "scott" || Q.Password.Secret() != passw || Q.ConnectString != connStr {
					t.Errorf("Parse(%q): user=%q password=%q connectString=%q, wanted the password %q",
						in, Q.Username, Q.Password.Secret(), Q.ConnectString, passw)
				}
			}
		}
	}
}
