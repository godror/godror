// +build go1.15

// Copyright 2019, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"database/sql/driver"
)

var _ = (driver.Validator)((*conn)(nil))
