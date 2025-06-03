// Copyright 2017, 2025 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

const DefaultBatchLimit = 1024

// Batch collects the Added rows and executes in batches, after collecting Limit number of rows.
// The default Limit is DefaultBatchLimit.
type Batch struct {
	Stmt        *sql.Stmt
	values      []interface{}
	rValues     []reflect.Value
	size, Limit int
}

// Add the values. The first call initializes the storage,
// so all the subsequent calls to Add must use the same number of values,
// with the same types.
//
// When the number of added rows reaches Size, Flush is called.
func (b *Batch) Add(ctx context.Context, values ...interface{}) error {
	if b.rValues == nil {
		if b.Limit <= 0 {
			b.Limit = DefaultBatchLimit
		}
		b.rValues = make([]reflect.Value, len(values))
	}
	func() {
		var i int
		var v any
		defer func() {
			if r := recover(); r != nil {
				panic(fmt.Errorf("append %#v (%d.) to %#v: %+v", v, i, b.rValues[i], r))
			}
		}()
		for i, v = range values {
			if !b.rValues[i].IsValid() {
				if v == nil { // a nil value has no type
					continue
				}
				b.rValues[i] = reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(v)), b.size, b.Limit)
			}
			if v == nil { // assume it has the same type as the other elements
				b.rValues[i] = reflect.Append(b.rValues[i], reflect.Zero(b.rValues[i].Type().Elem()))
				continue
			}
			rv := reflect.ValueOf(v)

			// type mismatch
			if rv.Type().Kind() != reflect.String &&
				b.rValues[i].Type().Elem().Kind() == reflect.String {
				vv := b.rValues[i]
				allZero := true
				for j := 0; j < vv.Len(); j++ {
					if allZero = vv.Index(j).Len() == 0; !allZero {
						break
					}
				}
				if allZero { // all zero, replace with proper typed slice
					b.rValues[i] = reflect.MakeSlice(reflect.SliceOf(rv.Type()), vv.Len(), vv.Cap())
				}
			}
			b.rValues[i] = reflect.Append(b.rValues[i], rv)
		}
	}()
	b.size++
	if b.size < b.Limit {
		return nil
	}
	err := b.Flush(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Size returns the buffered (unflushed) number of records.
func (b *Batch) Size() int { return b.size }

// FlushWithResult executes the statement, clears the storage, and returns the result.
func (b *Batch) FlushWithResult(ctx context.Context) (sql.Result, error) {
	if len(b.rValues) == 0 || b.rValues[0].Len() == 0 {
		return nil, nil
	}

	if b.values == nil {
		b.values = make([]interface{}, len(b.rValues))
	}
	for i, v := range b.rValues {
		if !v.IsValid() {
			b.values[i] = make([]string, b.size) // empty string == NULL
		} else {
			b.values[i] = v.Interface()
		}
	}

	result, err := b.Stmt.ExecContext(ctx, b.values...)
	if err != nil {
		return nil, err
	}

	_, rowsAffectedErr := result.RowsAffected()
	if rowsAffectedErr != nil {
		return nil, rowsAffectedErr
	}

	for i, v := range b.rValues {
		if v.IsValid() {
			b.rValues[i] = v.Slice(0, 0)
		}
	}
	b.size = 0

	return result, nil
}

// Flush executes the statement and clears the storage.
func (b *Batch) Flush(ctx context.Context) error {
	_, err := b.FlushWithResult(ctx)
	return err
}
