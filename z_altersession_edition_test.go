// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"database/sql"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestAlterSessionIdentifier checks that the alterSession parameters that
// take an identifier (EDITION, CONTAINER) work, alone and with others.
func TestAlterSessionIdentifier(t *testing.T) {
	ctx, cancel := testContext(t, time.Minute)
	defer cancel()
	const qry = `SELECT SYS_CONTEXT('USERENV', 'CURRENT_EDITION_NAME'), SYS_CONTEXT('USERENV', 'CON_NAME'),
		SYS_CONTEXT('USERENV', 'CDB_NAME'), TO_CHAR(DATE '2026-01-02') FROM DUAL`
	var edition, container, cdb sql.NullString
	var date string
	if err := testDb.QueryRowContext(ctx, qry).Scan(&edition, &container, &cdb, &date); err != nil {
		t.Fatal(err)
	}
	t.Logf("edition=%q container=%q cdb=%q", edition.String, container.String, cdb.String)

	for _, tC := range [][][2]string{
		{{"EDITION", "ORA$BASE"}},
		{{"edition", "ORA$BASE"}, {"NLS_DATE_FORMAT", `YYYY"|"MM`}},
		{{"NLS_DATE_FORMAT", `YYYY"|"MM`}, {"CONTAINER", container.String}},
	} {
		if tC[len(tC)-1][0] == "CONTAINER" && cdb.String == "" {
			t.Log("not a container database, skipping", tC)
			continue
		}
		P, err := godror.ParseDSN(testConStr)
		if err != nil {
			t.Fatal(err)
		}
		P.StandaloneConnection = godror.Bool(true)
		wantDate := date
		for _, kv := range tC {
			P.SetSessionParamOnInit(kv[0], kv[1])
			if kv[0] == "NLS_DATE_FORMAT" {
				wantDate = "2026|01"
			}
		}
		db := sql.OpenDB(godror.NewConnector(P))
		var gotEdition, gotContainer, gotDate string
		err = db.QueryRowContext(ctx, qry).Scan(&gotEdition, &gotContainer, &cdb, &gotDate)
		db.Close()
		if err != nil {
			t.Errorf("%v: %+v", tC, err)
			continue
		}
		if gotEdition != "ORA$BASE" || gotContainer != container.String || gotDate != wantDate {
			t.Errorf("%v: got edition=%q container=%q date=%q, wanted ORA$BASE, %q, %q",
				tC, gotEdition, gotContainer, gotDate, container.String, wantDate)
		}
	}
}
