// Copyright 2026 Tamás Gulácsi.
//
// SPDX-License-Identifier: AGPL-3.0

// Package sieve implements a bounded, concurrency-safe key-value cache using
// the SIEVE eviction policy.
package sieve

import (
	"container/list"
	"sync"
)

type (

	// Cache stores up to capacity key-value pairs. Get and Put are safe to call
	// concurrently. Entries are ordered by insertion, not by last access.
	Cache[K comparable, V any] struct {
		entries  map[K]*list.Element
		hand     *list.Element
		order    list.List // Front: newest; Back: oldest.
		capacity int
		mu       sync.Mutex
	}

	entry[K comparable, V any] struct {
		key     K
		value   V
		visited bool
	}
)

// NewCache creates a SIEVE cache with a fixed, positive capacity.
// NewCache panics if capacity is less than 1.
func NewCache[K comparable, V any](capacity int) *Cache[K, V] {
	if capacity < 1 {
		panic("sieve: capacity must be positive")
	}
	return &Cache[K, V]{
		capacity: capacity,
		entries:  make(map[K]*list.Element, capacity),
	}
}

// Get returns the value associated with key, and whether it was found.
// A hit marks the entry visited but does not change its position.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[key]; ok {
		e := elem.Value.(*entry[K, V])
		e.visited = true
		return e.value, true
	}
	var zero V
	return zero, false
}

// Put inserts or updates a key-value pair.
// An update marks the existing entry visited without changing its position.
// Inserting a new key into a full cache evicts one entry using SIEVE.
func (c *Cache[K, V]) Put(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[key]; ok {
		e := elem.Value.(*entry[K, V])
		e.value = value
		e.visited = true
		return
	}

	if len(c.entries) == c.capacity {
		c.evict()
	}

	elem := c.order.PushFront(&entry[K, V]{key: key, value: value})
	c.entries[key] = elem
}

// Len returns the current number of cached entries.
func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// evict removes the next unvisited entry, giving visited entries a second
// chance. The caller must hold c.mu and the cache must not be empty.
func (c *Cache[K, V]) evict() {
	candidate := c.hand
	if candidate == nil {
		candidate = c.order.Back()
	}

	for {
		e := candidate.Value.(*entry[K, V])
		if !e.visited {
			// Prev moves toward the newer entries, i.e. toward the front.
			// A nil hand means the next search starts at the oldest entry.
			c.hand = candidate.Prev()
			delete(c.entries, e.key)
			c.order.Remove(candidate)
			return
		}
		e.visited = false
		candidate = candidate.Prev()
		if candidate == nil {
			candidate = c.order.Back()
		}
	}
}

// Clear the cache, visiting all entries.
func (c *Cache[K, V]) Clear(f func(K, V)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if f != nil {
		for _, l := range c.entries {
			if e, ok := l.Value.(*entry[K, V]); ok {
				f(e.key, e.value)
			}
		}
	}
	clear(c.entries)
	c.order.Init()
	c.hand = nil
}
