// Copyright 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

/*
#include <stdlib.h>
#include "dpiImpl.h"
*/
import "C"

// JSON holds the JSON data to/from Oracle.
type JSONNode struct {
	dpiJsonNode C.dpiJsonNode
}

type JSONArray struct {
	dpiJsonArray C.dpiJsonArray
}

type JSONObject struct {
	dpiJsonObject C.dpiJsonObject
}
