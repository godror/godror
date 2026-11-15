// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// "" (what MarshalJSON writes for the empty Number) and null
// must unmarshal as the empty Number, which is bound as NULL.
func TestNumberJSONNull(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	type rec struct {
		N godror.Number
	}
	empty, err := json.Marshal(rec{})
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range []string{string(empty), `{"N":""}`, `{"N":null}`} {
		got := rec{N: "7"} // must be overwritten
		if err := json.Unmarshal([]byte(in), &got); err != nil {
			t.Errorf("unmarshal %s: %v", in, err)
			continue
		}
		if got.N != "" {
			t.Errorf("unmarshal %s: got %q, wanted the empty Number", in, got.N)
			continue
		}
		var back sql.NullString
		if err := testDb.QueryRowContext(ctx, "SELECT :1 FROM DUAL", got.N).Scan(&back); err != nil {
			t.Errorf("%s: bind: %v", in, err)
		} else if back.Valid {
			t.Errorf("%s: bound the empty Number, got %q back, wanted NULL", in, back.String)
		}
	}

	// The quotes alone do not make a number.
	for _, in := range []string{`"`, `"""`, `"-"x`, `nul`} {
		var n godror.Number
		if err := n.UnmarshalJSON([]byte(in)); err == nil {
			t.Errorf("unmarshal %s: no error, got %q", in, n)
		}
	}
}
