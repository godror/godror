// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"reflect"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestJSONBinaryFloat reads BINARY_FLOAT and BINARY_DOUBLE values stored in
// JSON. ODPI-C returns BINARY_FLOAT as a float32, which GetValue and String
// used to assert to float64.
func TestJSONBinaryFloat(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	serverVersion, err := godror.ServerVersion(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if serverVersion.Version < 21 {
		t.Skip("JSON type needs Oracle 21 or later")
	}

	for _, tc := range []struct {
		name, qry string
		want      any
		wantStr   string
	}{
		{"object_float",
			"SELECT JSON_OBJECT('a' VALUE TO_BINARY_FLOAT(1.5) RETURNING JSON) FROM DUAL",
			map[string]any{"a": float32(1.5)}, `{"a":1.5}`},
		{"array_float",
			"SELECT JSON_ARRAY(TO_BINARY_FLOAT(1.5), TO_BINARY_FLOAT(0.1) RETURNING JSON) FROM DUAL",
			[]any{float32(1.5), float32(0.1)}, `[1.5,0.1]`},
		{"scalar_float",
			"SELECT JSON_SCALAR(TO_BINARY_FLOAT(1.5)) FROM DUAL",
			float32(1.5), `1.5`},
		{"object_double",
			"SELECT JSON_OBJECT('a' VALUE TO_BINARY_DOUBLE(2.5) RETURNING JSON) FROM DUAL",
			map[string]any{"a": float64(2.5)}, `{"a":2.5}`},
		{"scalar_double",
			"SELECT JSON_SCALAR(TO_BINARY_DOUBLE(2.5)) FROM DUAL",
			float64(2.5), `2.5`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := conn.QueryContext(ctx, tc.qry)
			if err != nil {
				t.Fatalf("%s: %+v", tc.qry, err)
			}
			defer rows.Close()
			if !rows.Next() {
				t.Fatalf("%s: no rows: %+v", tc.qry, rows.Err())
			}
			var j godror.JSON
			if err = rows.Scan(&j); err != nil {
				t.Fatalf("scan: %+v", err)
			}
			for _, opt := range []godror.JSONOption{godror.JSONOptDefault, godror.JSONOptNumberAsString} {
				got, err := j.GetValue(opt)
				if err != nil {
					t.Fatalf("GetValue(%d): %+v", opt, err)
				}
				if !reflect.DeepEqual(got, tc.want) {
					t.Errorf("GetValue(%d): got %#v, wanted %#v", opt, got, tc.want)
				}
			}
			if got := j.String(); got != tc.wantStr {
				t.Errorf("String: got %q, wanted %q", got, tc.wantStr)
			}
		})
	}
}
