// Copyright 2017 Tamás Gulácsi
//
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package goracle

import (
	"context"
	"database/sql/driver"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
)

var (
	testCon     *conn
	testDrv     *drv
	testOpenErr error
	initOnce    sync.Once
)

func initConn() (*drv, *conn, error) {
	initOnce.Do(func() {
		testDrv = newDrv()
		dc, err := testDrv.Open(
			fmt.Sprintf("oracle://%s:%s@%s/?poolMinSessions=1&poolMaxSessions=4&poolIncrement=1&connectionClass=POOLED",
				os.Getenv("GORACLE_DRV_TEST_USERNAME"),
				os.Getenv("GORACLE_DRV_TEST_PASSWORD"),
				os.Getenv("GORACLE_DRV_TEST_DB"),
			),
		)
		if err != nil {
			testOpenErr = err
			return
		}
		testCon = dc.(*conn)
	})
	return testDrv, testCon, testOpenErr
}

func TestObjectDirect(t *testing.T) {
	_, testCon, err := initConn()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const crea = `CREATE OR REPLACE PACKAGE test_pkg_obj IS
  TYPE int_tab_typ IS TABLE OF PLS_INTEGER INDEX BY PLS_INTEGER;
  TYPE rec_typ IS RECORD (int PLS_INTEGER, num NUMBER, vc VARCHAR2(1000), c CHAR(1000), dt DATE);
  TYPE tab_typ IS TABLE OF rec_typ INDEX BY PLS_INTEGER;

  PROCEDURE modify(p_obj IN OUT NOCOPY tab_typ);
END;`
	const crea2 = `CREATE OR REPLACE PACKAGE BODY test_pkg_obj IS
  PROCEDURE modify(p_obj IN OUT NOCOPY tab_typ) IS
    v_idx PLS_INTEGER := NVL(p_obj.LAST, 0) + 1;
  BEGIN
    p_obj(v_idx).int := p_obj.COUNT;
    p_obj(v_idx).num := 314/100;
	p_obj(v_idx).vc  := 'abraka dabra';
	p_obj(v_idx).c   := 'X';
	p_obj(v_idx).dt  := SYSDATE;
  END modify;
END;`
	if err = prepExec(ctx, testCon, crea); err != nil {
		t.Fatal(err)
	}
	//defer prepExec(ctx, testCon, "DROP PACKAGE test_pkg_obj")
	if err = prepExec(ctx, testCon, crea2); err != nil {
		t.Fatal(err)
	}

	//defer tl.enableLogging(t)()
	clientVersion, _ := testCon.ClientVersion()
	serverVersion, _ := testCon.ServerVersion()
	t.Logf("clientVersion: %#v, serverVersion: %#v", clientVersion, serverVersion)
	cOt, err := testCon.GetObjectType(strings.ToUpper("test_pkg_obj.tab_typ"))
	if err != nil {
		if clientVersion.Version >= 12 && serverVersion.Version >= 12 {
			t.Fatal(fmt.Sprintf("%+v", err))
		}
		t.Log(err)
		t.Skipf("client=%d or server=%d < 12", clientVersion.Version, serverVersion.Version)
	}
	defer cOt.Close()
	t.Log(cOt)

	// create object from the type
	coll, err := cOt.NewCollection()
	if err != nil {
		t.Fatal(err)
	}
	defer coll.Close()

	// create an element object
	elt, err := cOt.CollectionOf.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer elt.Close()

	// append to the collection
	t.Logf("elt: %#v", elt)
	coll.AppendObject(elt)

	const mod = "BEGIN test_pkg_obj.modify(:1); END;"
	if err = prepExec(ctx, testCon, mod, driver.NamedValue{Ordinal: 1, Value: coll}); err != nil {
		t.Error(err)
	}
	t.Logf("coll: %#v", coll)
	t.Logf("elt : %#v", elt)
	for attr := range cOt.CollectionOf.Attributes {
		var data Data
		if err = elt.GetAttribute(&data, attr); err != nil {
			t.Error(err, attr)
		}
		t.Logf("elt.%s=%v", attr, data.Get())
	}
}

func prepExec(ctx context.Context, testCon *conn, qry string, args ...driver.NamedValue) error {
	stmt, err := testCon.PrepareContext(ctx, qry)
	if err != nil {
		return errors.Wrap(err, qry)
	}
	st := stmt.(*statement)
	_, err = st.ExecContext(ctx, args)
	stmt.Close()
	return errors.Wrap(err, qry)
}
