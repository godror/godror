// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package dsn

import "testing"

// The URL form must understand the parameters that the logfmt form does.
func TestParseURLParams(t *testing.T) {
	L, err := Parse("user=scott password=secret connectString=host:1521/svc charset=WE8MSWIN1252 token=tok privateKey=pk")
	if err != nil {
		t.Fatal(err)
	}
	U, err := Parse("oracle://scott:secret@host:1521/svc?charset=WE8MSWIN1252&token=tok&privateKey=pk")
	if err != nil {
		t.Fatal(err)
	}
	if L.Charset != "WE8MSWIN1252" || L.Token != "tok" || L.PrivateKey != "pk" {
		t.Errorf("logfmt form: charset=%q token=%q privateKey=%q", L.Charset, L.Token, L.PrivateKey)
	}
	if U.Charset != L.Charset || U.Token != L.Token || U.PrivateKey != L.PrivateKey {
		t.Errorf("URL form: charset=%q token=%q privateKey=%q, wanted %q %q %q",
			U.Charset, U.Token, U.PrivateKey, L.Charset, L.Token, L.PrivateKey)
	}
	if got, want := U.StringWithPassword(), L.StringWithPassword(); got != want {
		t.Errorf("URL form parsed as\n%s\nlogfmt form as\n%s", got, want)
	}

	// The last one wins for a repeated key, as before.
	if L, err = Parse("connectString=db charset=A charset=B"); err != nil {
		t.Fatal(err)
	} else if L.Charset != "B" {
		t.Errorf("charset=%q, wanted B", L.Charset)
	}
}
