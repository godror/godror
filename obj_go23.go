//go:build go1.23

// Copyright 2019, 2025 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"errors"
	"iter"
)

func (O ObjectCollection) Items() iter.Seq2[*Data, error] {
	return func(yield func(*Data, error) bool) {
		data := scratch.Get()
		defer scratch.Put(data)
		for curr, err := O.First(); err == nil; curr, err = O.Next(curr) {
			err := O.GetItem(data, curr)
			if !yield(data, err) {
				break
			}
		}
	}
}

func (O ObjectCollection) Indexes() iter.Seq2[int, error] {
	i, err := O.First()
	return func(yield func(i int, err error) bool) {
		if err != nil {
			yield(i, err)
			return
		}
		i, err = O.Next(i)
		if err != nil {
			if !errors.Is(err, ErrNotExist) {
				yield(i, err)
			}
			return
		}
		if !yield(i, err) {
			return
		}
	}
}
