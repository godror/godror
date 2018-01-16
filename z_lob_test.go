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

package goracle_test

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"testing"
	"time"

	"github.com/pkg/errors"

	goracle "gopkg.in/goracle.v2"
)

func TestStatWithLobs(t *testing.T) {
	t.Parallel()
	//defer tl.enableLogging(t)()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ms, err := newMetricSet(ctx, testDb)
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	if _, err = ms.Fetch(ctx); err != nil {
		if c, ok := errors.Cause(err).(interface{ Code() int }); ok && c.Code() == 942 {
			t.Skip(err)
			return
		}
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		if err := ctx.Err(); err != nil {
			break
		}
		events, err := ms.Fetch(ctx)
		t.Log("events:", len(events))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func newMetricSet(ctx context.Context, db *sql.DB) (*metricSet, error) {
	qry := "select /* metricset: sqlstats */ inst_id, sql_fulltext, last_active_time from gv$sqlstats WHERE ROWNUM < 11"
	stmt, err := db.PrepareContext(ctx, qry)
	if err != nil {
		return nil, err
	}

	return &metricSet{
		stmt: stmt,
	}, nil
}

type metricSet struct {
	stmt *sql.Stmt
}

func (m *metricSet) Close() error {
	st := m.stmt
	m.stmt = nil
	if st == nil {
		return nil
	}
	return st.Close()
}

// Fetch methods implements the data gathering and data conversion to the right format
// It returns the event which is then forward to the output. In case of an error, a
// descriptive error must be returned.
func (m *metricSet) Fetch(ctx context.Context) ([]event, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := m.stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []event
	var buf bytes.Buffer
	for rows.Next() {
		var e event
		var lob *goracle.Lob
		if err := rows.Scan(&e.ID, &lob, &e.LastActive); err != nil {
			return events, err
		}
		buf.Reset()
		if _, err := io.Copy(&buf, lob); err != nil {
			return events, err
		}
		e.Text = buf.String()
		events = append(events, e)
	}

	return events, nil
}

type event struct {
	ID         int64
	Text       string
	LastActive time.Time
}
