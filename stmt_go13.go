// +build go1.13

// Copyright 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import "database/sql"

type NullTime = sql.NullTime

var nullTime interface{} = nil
