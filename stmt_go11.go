// +build !go1.13

// Copyright 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"database/sql/driver"
	"fmt"
	"time"
)

// NullTime represents a time.Time that may be null. NullTime implements the Scanner interface so it can be used as a scan destination, similar to NullString.
//
// Copied from Go 1.13
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

// Scan implements the Scanner interface.
//
// Copied from Go 1.13
func (n *NullTime) Scan(value interface{}) error {
	if value == nil {
		n.Time, n.Valid = time.Time{}, false
		return nil
	}
	n.Valid = true
	switch x := value.(type) {
	case time.Time:
		n.Time = x
	default:
		return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type %T", value, &n.Time)
	}
	return nil
}

// Value implements the driver Valuer interface.
//
// Copied from Go 1.13
func (n NullTime) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Time, nil
}

var nullTime interface{} = time.Time{}
