// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestOpenConnectorDriver checks that OpenConnector of a non-default driver
// returns a connector of that driver, not of the default one.
func TestOpenConnectorDriver(t *testing.T) {
	d := godror.NewDriver()
	defer d.Close()
	c, err := d.OpenConnector(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	if c.Driver() != driver.Driver(d) {
		t.Errorf("OpenConnector of driver %p returned a connector of driver %p", d, c.Driver())
	}

	ctx, cancel := testContext(t, 30*time.Second)
	defer cancel()
	db := sql.OpenDB(c)
	defer db.Close()
	var n int
	if err = db.QueryRowContext(ctx, "SELECT 1 FROM DUAL").Scan(&n); err != nil {
		t.Fatal(err)
	}
}
