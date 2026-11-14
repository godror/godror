package godror_test

import (
	"context"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestGetBytesOutlivesAttribute checks that the slice returned by
// Data.GetBytes keeps its contents after the object attribute it was read
// from changes or the object is closed.
func TestGetBytesOutlivesAttribute(t *testing.T) {
	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()

	typ := "test_getbytes_t" + tblSuffix
	if _, err := testDb.ExecContext(ctx, "CREATE OR REPLACE TYPE "+typ+" AS OBJECT (s VARCHAR2(100))"); err != nil {
		t.Fatal(err)
	}
	defer testDb.ExecContext(context.Background(), "DROP TYPE "+typ)

	cx, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cx.Close()
	var got []string
	if err := cx.Raw(func(driverConn any) error {
		conn := driverConn.(godror.Conn)
		ot, err := conn.GetObjectType(typ)
		if err != nil {
			return err
		}
		defer ot.Close()
		obj, err := ot.NewObject()
		if err != nil {
			return err
		}
		var data godror.Data
		var slices [][]byte
		for _, s := range []string{"first value", "other value", "third value"} {
			if err := obj.Set("S", s); err != nil {
				obj.Close()
				return err
			}
			if err := obj.GetAttribute(&data, "S"); err != nil {
				obj.Close()
				return err
			}
			slices = append(slices, data.GetBytes())
		}
		obj.Close()
		for _, b := range slices {
			got = append(got, string(b))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"first value", "other value", "third value"} {
		if got[i] != want {
			t.Errorf("%d. got %q, wanted %q", i, got[i], want)
		}
	}
}
