// Copyright 2024 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package cloexec

import (
	"net/http"
	"testing"
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

	connections, err := getConnections("")
	if err != nil {
		t.Fatal(err)
	}
	if len(connections) == 0 {
		t.Error("no connections")
	}
	testLogf = t.Logf
	for _, fd := range connections {
		t.Log(fd)
		if isSet, err := getFd(uintptr(fd)); err != nil {
			t.Errorf("%d: %+v", fd, err)
		} else if !isSet {
			t.Errorf("%d: not CLOEXEC?", fd)
		} else if err = setFd(uintptr(fd), false); err != nil {
			t.Errorf("setFd(%d, false): %+v", fd, err)
		} else if isSet, err = getFd(uintptr(fd)); err != nil {
			t.Errorf("unset %d: %+v", fd, err)
		} else if isSet {
			t.Errorf("%d: CLOEXEC?", fd)
		} else if err = setFd(uintptr(fd), true); err != nil {
			t.Errorf("set %d: %+v", fd, err)
		} else if isSet, err = getFd(uintptr(fd)); err != nil {
			t.Errorf("get %d: %+v", fd, err)
		} else if !isSet {
			t.Errorf("%d: wanted CLOEXEC, got %t", fd, isSet)
		}
	}
}
