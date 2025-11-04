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
