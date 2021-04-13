// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"testing"
)

func TestNewDriverSepContext(t *testing.T) {
	if oneContext {
		if ok, _ := strconv.ParseBool(os.Getenv("GODROR_SEPARATE_CONTEXT")); !ok {
			t.Skip("GODROR_SEPARATE_CONTEXT is not set, skipping TestNewDriverSepContext")
		}
	}
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
	errInfo := newErrorInfo(0, "ORA-24315: érvénytelen attribútumtípus\n")
	t.Logf("errInfo: %#v", errInfo)
	err := fromErrorInfo(errInfo)
	t.Log("OraErr", err)
	var oe interface{ Code() int }
	if !errors.As(err, &oe) || oe.Code() != 24315 {
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
