// Copyright 2018, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func TestHeterogeneousPoolIntegration(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("HeterogeneousPoolIntegration"), 30*time.Second)
	defer cancel()

	const proxyPassword = "myPassword666myPassword"
	const proxyUser = "test_proxyUser"

	cs, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	cs.Heterogeneous = true
	cs.EnableEvents = false
	username := cs.Username
	testHeterogeneousConStr := cs.StringWithPassword()
	t.Log(testHeterogeneousConStr)

	var testHeterogeneousDB *sql.DB
	if testHeterogeneousDB, err = sql.Open("godror", testHeterogeneousConStr); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", testHeterogeneousConStr, err))
	}
	defer testHeterogeneousDB.Close()
	testHeterogeneousDB.SetMaxIdleConns(0)

	// Check that it works
	conn, err := testHeterogeneousDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.ExecContext(ctx, fmt.Sprintf("DROP USER %s", proxyUser))

	for _, qry := range []string{
		fmt.Sprintf("CREATE USER %s IDENTIFIED BY "+proxyPassword, proxyUser),
		fmt.Sprintf("GRANT CREATE SESSION TO %s", proxyUser),
		fmt.Sprintf("ALTER USER %s GRANT CONNECT THROUGH %s", proxyUser, username),
	} {
		if _, err = conn.ExecContext(ctx, qry); err != nil {
			if strings.Contains(err.Error(), "ORA-01031:") {
				t.Log("Please issue this:\nGRANT CREATE USER, DROP USER, ALTER USER TO " + username + ";\n" +
					"GRANT CREATE SESSION TO " + username + " WITH ADMIN OPTION;\n")
			}
			t.Skip(fmt.Errorf("%s: %w", qry, err))
		}
	}
	defer func() {
		testHeterogeneousDB.ExecContext(testContext("HeterogeneousPoolIntegration-drop"), "DROP USER "+proxyUser)
	}()

	for tName, tCase := range map[string]struct {
		In   context.Context
		Want string
	}{
		"noContext":       {In: ctx, Want: username},
		"proxyUser":       {In: godror.ContextWithUserPassw(ctx, proxyUser, proxyPassword, ""), Want: proxyUser},
		"proxyUserNoPass": {In: godror.ContextWithUserPassw(ctx, proxyUser, "", ""), Want: proxyUser},
	} {
		t.Run(tName, func(t *testing.T) {
			var result string
			if err = testHeterogeneousDB.QueryRowContext(tCase.In, "SELECT user FROM dual").Scan(&result); err != nil {
				t.Fatal(err)
			}
			if !strings.EqualFold(tCase.Want, result) {
				t.Errorf("%s: currentUser got %s, wanted %s", tName, result, tCase.Want)
			}
		})

	}

}

func TestContextWithUserPassw(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("ContextWithUserPassw"), 30*time.Second)
	defer cancel()

	cs, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	cs.Heterogeneous = true
	username, password := cs.Username, cs.Password
	cs.Username = ""
	cs.Password.Reset()
	testHeterogeneousConStr := cs.StringWithPassword()
	t.Log(testConStr, " -> ", testHeterogeneousConStr)

	var testHeterogeneousDB *sql.DB
	if testHeterogeneousDB, err = sql.Open("godror", testHeterogeneousConStr); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", testHeterogeneousConStr, err))
	}
	defer testHeterogeneousDB.Close()

	ctx = godror.ContextWithUserPassw(ctx, username, password.Secret(), "")
	if err := testHeterogeneousDB.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log(ctx)
}
