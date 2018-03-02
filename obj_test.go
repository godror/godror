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
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
)

var (
	testCon                      *conn
	testDrv                      *drv
	testOpenErr                  error
	clientVersion, serverVersion VersionInfo
	initOnce                     sync.Once
)

func initConn() (*drv, *conn, error) {
	initOnce.Do(func() {
		if testDrv, testOpenErr = newDrv(); testOpenErr != nil {
			return
		}
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

	qry := `CREATE OR REPLACE PACKAGE test_pkg_obj IS
  TYPE int_tab_typ IS TABLE OF PLS_INTEGER INDEX BY PLS_INTEGER;
  TYPE rec_typ IS RECORD (int PLS_INTEGER, num NUMBER, vc VARCHAR2(1000), c CHAR(1000), dt DATE);
  TYPE tab_typ IS TABLE OF rec_typ INDEX BY PLS_INTEGER;
END;`
	if err = prepExec(ctx, testCon, qry); err != nil {
		t.Fatal(errors.Wrap(err, qry))
	}
	defer prepExec(ctx, testCon, "DROP PACKAGE test_pkg_obj")

	//defer tl.enableLogging(t)()
	ot, err := testCon.GetObjectType("test_pkg_obj.tab_typ")
	if err != nil {
		if clientVersion.Version >= 12 && serverVersion.Version >= 12 {
			t.Fatal(fmt.Sprintf("%+v", err))
		}
		t.Log(err)
		t.Skip("client or server < 12")
	}
	t.Log(ot)
}

func prepExec(ctx context.Context, testCon *conn, qry string, args ...driver.NamedValue) error {
	stmt, err := testCon.PrepareContext(ctx, qry)
	if err != nil {
		return errors.Wrap(err, qry)
	}
	defer stmt.Close()
	st := stmt.(*statement)
	_, err = st.ExecContext(ctx, args)
	return err
}
