// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package dsn

import (
	"strings"
	"testing"
)

// The " AS SYSDBA" suffix was located in the upper-cased DSN and cut from the
// original one, though upper-casing can change the length of the text.
func TestParseAsSysdbaNonASCII(t *testing.T) {
	for _, tC := range []struct {
		in                   string
		user, passw, connStr string
		role                 AdminRole
	}{
		// U+0250 is 2 bytes, its upper case is 3 bytes: this one panicked.
		{in: "sys/secret@" + strings.Repeat("ɐ", 12) + " as sysdba",
			user: "sys", passw: "secret", connStr: strings.Repeat("ɐ", 12), role: SysDBA},
		// U+0131 is 2 bytes, its upper case is 1 byte: these were cut short.
		{in: "sys/şıfre as sysdba", user: "sys", passw: "şıfre", role: SysDBA},
		{in: "sys/secret@dıııb AS SysOper", user: "sys", passw: "secret", connStr: "dıııb", role: SysOPER},
		{in: "sys/sıfre@dıııb", user: "sys", passw: "sıfre", connStr: "dıııb"},
		// plain ASCII, as before
		{in: "sys/secret@db as sysasm", user: "sys", passw: "secret", connStr: "db", role: SysASM},
		{in: "sys/secret As SysDBA", user: "sys", passw: "secret", role: SysDBA},
		{in: "scott/secret@db as nobody", user: "scott", passw: "secret", connStr: "db as nobody"},
	} {
		P, err := func() (P ConnectionParams, err error) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Parse(%q) panics: %v", tC.in, r)
				}
			}()
			return Parse(tC.in)
		}()
		if err != nil {
			t.Errorf("Parse(%q): %v", tC.in, err)
			continue
		}
		if P.Username != tC.user || P.Password.Secret() != tC.passw || P.ConnectString != tC.connStr || P.AdminRole != tC.role {
			t.Errorf("Parse(%q): user=%q password=%q connectString=%q adminRole=%q, wanted %q %q %q %q",
				tC.in, P.Username, P.Password.Secret(), P.ConnectString, P.AdminRole,
				tC.user, tC.passw, tC.connStr, tC.role)
		}
	}
}
