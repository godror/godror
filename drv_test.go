// Copyright 2017 Tamás Gulácsi
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package goracle

import (
	"encoding/json"
	"testing"
)

func TestFromErrorInfo(t *testing.T) {
	errInfo := newErrorInfo(0, "ORA-24315: érvénytelen attribútumtípus\n")
	t.Log("errInfo", errInfo)
	oe := fromErrorInfo(errInfo)
	t.Log("OraErr", oe)
	if oe.Code() != 24315 {
		t.Errorf("got %d, wanted 24315", oe.Code())
	}
}

func TestMarshalJSON(t *testing.T) {
	n := Number("12345.6789")
	b, err := (&n).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	n = Number("")
	if err = n.UnmarshalJSON(b); err != nil {
		t.Fatal(err)
	}
	t.Log(n.String())

	n = Number("")
	b, err = json.Marshal(struct {
		N Number
		A int
	}{N: n, A: 12})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(b))

	type myStruct struct {
		N interface{}
		A int
	}
	n = Number("")
	ttt := myStruct{N: &n, A: 12}
	b, err = json.Marshal(ttt)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(b))
}
