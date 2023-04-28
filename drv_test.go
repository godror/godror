// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestNewDriverSepContext(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Log("i:", i)
		d := &drv{}
		if cx, err := d.Open("tiger/scott"); err == nil {
			cx.Close()
		}
		defer d.Close()
	}
}

func TestNewDriver(t *testing.T) {
	for i := 0; i < 10; i++ {
		drv := NewDriver()
		if cx, err := drv.Open("tiger/scott"); err == nil {
			cx.Close()
		}
		defer drv.Close()
	}
}

func TestFromErrorInfo(t *testing.T) {
	for _, tC := range []struct {
		Msg      string
		WantMsg  string
		WantCode int
	}{
		{Msg: "ORA-24315: érvénytelen attribútumtípus\n",
			WantCode: 24315, WantMsg: "érvénytelen attribútumtípus"},
		{Msg: "DPI-1080: connection was closed by ORA-3113",
			WantCode: 3113, WantMsg: "DPI-1080: connection was closed by"},
	} {
		tC := tC
		t.Run(tC.Msg, func(t *testing.T) {
			errInfo := newErrorInfo(0, tC.Msg)
			t.Logf("errInfo: %#v", errInfo)
			err := fromErrorInfo(errInfo)
			t.Log("OraErr", err)
			var oe interface {
				Code() int
				Message() string
			}
			if !errors.As(err, &oe) {
				t.Fatal("not OraErr")
			}
			if oe.Code() != tC.WantCode {
				t.Errorf("got %d, wanted %d", oe.Code(), tC.WantCode)
			}
			if oe.Message() != tC.WantMsg {
				t.Errorf("got %q, wanted %q", oe.Message(), tC.WantMsg)
			}
		})
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
