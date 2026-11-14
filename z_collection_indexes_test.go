// Copyright 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"slices"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestObjectCollectionIndexes checks that Indexes yields every index of the
// collection, from First through Next, including sparse nested tables.
func TestObjectCollectionIndexes(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	typName := "TEST_COLLIDX_NT" + tblSuffix
	conn.ExecContext(ctx, "DROP TYPE "+typName)
	if _, err = conn.ExecContext(ctx, "CREATE OR REPLACE TYPE "+typName+" AS TABLE OF NUMBER"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TYPE "+typName)

	ot, err := godror.GetObjectType(ctx, conn, typName)
	if err != nil {
		t.Fatal(err)
	}
	defer ot.Close()

	indexes := func(t *testing.T, coll godror.ObjectCollection) []int {
		t.Helper()
		var got []int
		for i, err := range coll.Indexes() {
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, i)
		}
		return got
	}

	for _, tc := range []struct {
		name   string
		n      int
		delete []int
		want   []int
	}{
		{"empty", 0, nil, nil},
		{"one", 1, nil, []int{0}},
		{"three", 3, nil, []int{0, 1, 2}},
		{"sparse", 3, []int{1}, []int{0, 2}},
		{"sparse_first", 4, []int{0, 2}, []int{1, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coll, err := ot.NewCollection()
			if err != nil {
				t.Fatal(err)
			}
			defer coll.Close()
			for i := 0; i < tc.n; i++ {
				if err = coll.Append(i + 1); err != nil {
					t.Fatal(err)
				}
			}
			for _, i := range tc.delete {
				if err = coll.Delete(i); err != nil {
					t.Fatal(err)
				}
			}
			if got := indexes(t, coll); !slices.Equal(got, tc.want) {
				t.Errorf("Indexes: got %v, wanted %v", got, tc.want)
			}
			// The iterator can be used more than once.
			if got := indexes(t, coll); !slices.Equal(got, tc.want) {
				t.Errorf("Indexes (2nd): got %v, wanted %v", got, tc.want)
			}
			// Stopping early must not call yield again.
			if len(tc.want) > 1 {
				var got []int
				for i, err := range coll.Indexes() {
					if err != nil {
						t.Fatal(err)
					}
					got = append(got, i)
					break
				}
				if !slices.Equal(got, tc.want[:1]) {
					t.Errorf("Indexes with break: got %v, wanted %v", got, tc.want[:1])
				}
			}
		})
	}
}
