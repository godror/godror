// Copyright 2022 The Godror Authors
// Copyright (c) 2019 Josh Bleecher Snyder
//
// SPDX-License-Identifier: MIT

// Interning is best effort only.
// Interned strings may be removed automatically
// at any time without notification.
// All functions may be called concurrently
// with themselves and each other.
package godror

import "sync"

var (
	internStringPool sync.Pool = sync.Pool{
		New: func() interface{} {
			return make(map[string]string)
		},
	}
	internNumberPool sync.Pool = sync.Pool{
		New: func() interface{} {
			return make(map[Number]Number)
		},
	}
)

// internString returns s, interned.
func internString(s string) string {
	m := internStringPool.Get().(map[string]string)
	c, ok := m[s]
	if ok {
		internStringPool.Put(m)
		return c
	}
	m[s] = s
	internStringPool.Put(m)
	return s
}

var _ = internString

// internBytes returns b converted to a string, interned.
func internBytes(b []byte) string {
	m := internStringPool.Get().(map[string]string)
	c, ok := m[string(b)]
	if ok {
		internStringPool.Put(m)
		return c
	}
	s := string(b)
	m[s] = s
	internStringPool.Put(m)
	return s
}

// internNumberBytes returns b converted to a Number, interned.
func internNumberBytes(b []byte) Number {
	m := internNumberPool.Get().(map[Number]Number)
	c, ok := m[Number(b)]
	if ok {
		internNumberPool.Put(m)
		return c
	}
	s := Number(b)
	m[s] = s
	internNumberPool.Put(m)
	return s
}
