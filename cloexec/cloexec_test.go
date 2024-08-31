// Copyright 2024 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package cloexec

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestSetNetConnections(t *testing.T) {
	_, err := http.Get("https://google.com")
	if err != nil {
		t.Fatal(err)
	}

	testLogf = t.Logf
	if err := SetNetConnections("tcp"); err != nil {
		t.Error(err)
	}

}
func TestGetFd(t *testing.T) {
	_, err := http.Get("https://google.com")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	connections, err := getConnections(ctx, "")
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if len(connections) == 0 {
		t.Error("no connections")
	}
	testLogf = t.Logf
	for _, c := range connections {
		t.Log(c.Fd)
		if isSet, err := getFd(uintptr(c.Fd)); err != nil {
			t.Errorf("%d: %+v", c.Fd, err)
		} else if !isSet {
			t.Errorf("%d: not CLOEXEC?", c.Fd)
		} else if err = setFd(uintptr(c.Fd), false); err != nil {
			t.Errorf("setFd(%d, false): %+v", c.Fd, err)
		} else if isSet, err = getFd(uintptr(c.Fd)); err != nil {
			t.Errorf("unset %d: %+v", c.Fd, err)
		} else if isSet {
			t.Errorf("%d: CLOEXEC?", c.Fd)
		} else if err = setFd(uintptr(c.Fd), true); err != nil {
			t.Errorf("set %d: %+v", c.Fd, err)
		} else if isSet, err = getFd(uintptr(c.Fd)); err != nil {
			t.Errorf("get %d: %+v", c.Fd, err)
		} else if !isSet {
			t.Errorf("%d: wanted CLOEXEC, got %t", c.Fd, isSet)
		}
	}
}
