// Copyright 2019, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

type execer interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

const msgCount = 3 * maxSessions

func TestQueue(t *testing.T) {
	P, _ := godror.ParseConnString(testConStr)
	if P.IsStandalone() {
		// Sometimes it fails with pooled sessions.
		t.Parallel()
	}
	ctx, cancel := context.WithTimeout(testContext("Queue"), 30*time.Second)
	defer cancel()

	t.Run("raw", func(t *testing.T) {
		const qName = "TEST_Q"
		const qTblName = qName + "_TBL"

		testQueue(ctx, t, qName, "",
			func(ctx context.Context, db execer, user string) error {
				qry := `DECLARE
		tbl CONSTANT VARCHAR2(61) := '` + user + "." + qTblName + `';
		q CONSTANT VARCHAR2(61) := '` + user + "." + qName + `';
	BEGIN
		BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
		BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
		BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl); EXCEPTION WHEN OTHERS THEN NULL; END;

		SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, 'RAW');
		SYS.DBMS_AQADM.CREATE_QUEUE(q, tbl);
		SYS.DBMS_AQADM.grant_queue_privilege('ENQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.grant_queue_privilege('DEQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.start_queue(q);
	END;`
				_, err := db.ExecContext(ctx, qry)
				return err
			},

			func(ctx context.Context, db execer, user string) error {
				db.ExecContext(
					ctx,
					`DECLARE
			tbl CONSTANT VARCHAR2(61) := USER||'.'||:1;
			q CONSTANT VARCHAR2(61) := USER||'.'||:2;
		BEGIN
			BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl); EXCEPTION WHEN OTHERS THEN NULL;
		END;`,
					qTblName, qName,
				)
				return nil
			},

			func(_ *godror.Queue, i int) (godror.Message, string) {
				s := fmt.Sprintf("%03d. árvíztűrő tükörfúrógép", i)
				return godror.Message{Raw: []byte(s)}, s
			},

			func(m godror.Message, i int) (string, error) {
				if len(m.Raw) == 0 {
					t.Logf("%d. received empty message: %#v", i, m)
					return "", nil
				}
				return string(m.Raw), nil
			},
		)
	})

	t.Run("obj", func(t *testing.T) {
		const qName = "TEST_QOBJ"
		const qTblName = qName + "_TBL"
		const qTypName = qName + "_TYP"
		const arrTypName = qName + "_ARR_TYP"
		testDb.ExecContext(ctx, "DROP TYPE "+qTypName)
		testDb.ExecContext(ctx, "DROP TYPE "+arrTypName)

		var data godror.Data
		testQueue(ctx, t, qName, qTypName,
			func(ctx context.Context, db execer, user string) error {
				var plus strings.Builder
				for _, qry := range []string{
					"CREATE OR REPLACE TYPE " + user + "." + arrTypName + " IS TABLE OF VARCHAR2(1000)",
					"CREATE OR REPLACE TYPE " + user + "." + qTypName + " IS OBJECT (f_vc20 VARCHAR2(20), f_num NUMBER, f_dt DATE/*, f_arr " + arrTypName + "*/)",
				} {
					if _, err := db.ExecContext(ctx, qry); err != nil {
						t.Logf("%s: %+v", qry, err)
						if strings.HasPrefix(qry, "CREATE ") || !strings.Contains(err.Error(), "not exist") {
							return err
						}
					}
					plus.WriteString(qry)
					plus.WriteString(";\n")
				}
				{
					qry := `DECLARE
		tbl CONSTANT VARCHAR2(61) := '` + user + "." + qTblName + `';
		q CONSTANT VARCHAR2(61) := '` + user + "." + qName + `';
		typ CONSTANT VARCHAR2(61) := '` + user + "." + qTypName + `';
	BEGIN
		SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, typ);
		SYS.DBMS_AQADM.CREATE_QUEUE(q, tbl);

		SYS.DBMS_AQADM.grant_queue_privilege('ENQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.grant_queue_privilege('DEQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.start_queue(q);
	END;`
					if _, err := db.ExecContext(ctx, qry); err != nil {
						t.Logf("%v", fmt.Errorf("%s: %w", qry, err))
					}
				}

				return nil
			},

			func(ctx context.Context, db execer, user string) error {
				qry := `DECLARE
			tbl CONSTANT VARCHAR2(61) := USER||'.'||:1;
			q CONSTANT VARCHAR2(61) := USER||'.'||:2;
		BEGIN
			BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN EXECUTE IMMEDIATE 'DROP TABLE '||tbl; EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN EXECUTE IMMEDIATE 'DROP TYPE ` + user + `.` + qTypName + ` FORCE'; EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN EXECUTE IMMEDIATE 'DROP TYPE ` + user + `.` + arrTypName + ` FORCE'; EXCEPTION WHEN OTHERS THEN NULL; END;
		END;`
				if _, err := db.ExecContext(ctx, qry, qTblName, qName); err != nil {
					t.Logf("%q: %+v", qry, err)
				}
				return nil
			},

			func(q *godror.Queue, i int) (godror.Message, string) {
				obj, err := q.PayloadObjectType.NewObject()
				if err != nil {
					t.Fatalf("%d. %+v", i, err)
				}
				t.Logf("obj=%#v", obj)
				if err = obj.Set("F_DT", time.Now()); err != nil {
					t.Fatal(err)
				}
				if err = obj.Set("F_VC20", "árvíztűrő"); err != nil {
					t.Fatal(err)
				}

				if godror.Log != nil {
					godror.Log("msg", "Set F_NUM", "i", i)
				}
				if err = obj.Set("F_NUM", int64(i)); err != nil {
					t.Fatal(err)
				}
				if godror.Log != nil {
					godror.Log("msg", "Get F_NUM", "data", data)
				}
				if err = obj.GetAttribute(&data, "F_NUM"); err != nil {
					t.Fatal(err)
				}
				k := int(data.GetFloat64())
				if godror.Log != nil {
					godror.Log("msg", "Get F_NUM", "data", data, "k", k)
				}
				if k != i {
					t.Fatalf("got %d, wanted %d", k, i)
				}
				return godror.Message{Object: obj}, strconv.Itoa(k)
			},

			func(m godror.Message, i int) (string, error) {
				var data godror.Data
				if m.Object == nil {
					t.Logf("%d. received empty message: %#v", i, m)
					return "", nil
				}
				if err := m.Object.GetAttribute(&data, "F_NUM"); err != nil {
					return "", err
				}
				m.Object.Close()
				s := int(data.GetFloat64())
				//defer m.Object.ObjectType.Close()
				t.Logf("%d: got: %q", i, s)
				return strconv.Itoa(s), nil
			},
		)
	})

}

func testQueue(
	ctx context.Context, t *testing.T,
	qName, objName string,
	setUp, tearDown func(ctx context.Context, db execer, user string) error,
	newMessage func(*godror.Queue, int) (godror.Message, string),
	checkMessage func(godror.Message, int) (string, error),
) {
	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	var user string
	if err := testDb.QueryRowContext(ctx, "SELECT USER FROM DUAL").Scan(&user); err != nil {
		t.Fatal(err)
	}

	if err = tearDown(ctx, tx, user); err != nil {
		t.Log(err)
	}
	if err = setUp(ctx, tx, user); err != nil {
		if strings.Contains(err.Error(), "PLS-00201: identifier 'SYS.DBMS_AQADM' must be declared") {
			t.Skip(err.Error())
		}
		t.Fatalf("%+v", err)
	}
	defer func() {
		if err = tearDown(testContext("queue-teardown"), testDb, user); err != nil {
			t.Log(err)
		}
	}()

	q, err := godror.NewQueue(ctx, tx, qName, objName, godror.WithEnqOptions(godror.EnqOptions{
		Visibility:   godror.VisibleOnCommit,
		DeliveryMode: godror.DeliverPersistent,
	}))
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer q.Close()

	t.Logf("name=%q obj=%q q=%#v", q.Name(), objName, q)
	enqOpts, err := q.EnqOptions()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("enqOpts: %#v", enqOpts)
	deqOpts, err := q.DeqOptions()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("deqOpts: %#v", deqOpts)

	want := make([]string, 0, msgCount)
	seen := make(map[string]int, msgCount)

	// Put some messages into the queue
	msgs := make([]godror.Message, 2)
	for i := 0; i < msgCount; {
		for j := range msgs {
			var s string
			msgs[j], s = newMessage(q, i)
			msgs[j].Expiration = 10 * time.Second
			want = append(want, s)
			i++
		}
		if err = q.Enqueue(msgs); err != nil {
			var ec interface{ Code() int }
			if errors.As(err, &ec) && ec.Code() == 24444 {
				t.Skip(err)
			}
			t.Fatal("enqueue:", err)
		}
		if objName != "" {
			for _, m := range msgs {
				if m.Object != nil {
					m.Object.Close()
				}
			}
		}

		// Let's test enqOne
		if i > msgCount/3 {
			msgs = msgs[:1]
		}
	}
	t.Logf("enqueued %d messages", msgCount)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	msgs = msgs[:cap(msgs)]
	for i := 0; i < msgCount; {
		n := func(i int) int {
			tx, err := testDb.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			// Let's test deqOne
			if i == msgCount/3 {
				msgs = msgs[:1]
			}
			q, err := godror.NewQueue(ctx, tx, qName, objName,
				godror.WithDeqOptions(godror.DeqOptions{
					Mode:       godror.DeqRemove,
					Visibility: godror.VisibleOnCommit,
					Navigation: godror.NavNext,
					Wait:       1 * time.Second,
				}))
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			//t.Logf("name=%q q=%#v", q.Name(), q)
			n, err := q.Dequeue(msgs)
			if err != nil {
				t.Error("dequeue:", err)
			}
			t.Logf("%d. received %d message(s)", i, n)
			if n == 0 {
				return 0
			}
			for j, m := range msgs[:n] {
				s, err := checkMessage(m, i+j)
				if err != nil {
					t.Error(err)
				}
				t.Logf("%d: got: %q", i+j, s)
				if k, ok := seen[s]; ok {
					t.Fatalf("%d. %q already seen in %d", i, s, k)
				}
				seen[s] = i
			}
			//i += n
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			return n
		}(i)
		i += n
		if n == 0 {
			break
		}
	}

	PrintConnStats()

	t.Logf("seen: %v", seen)
	notSeen := make([]string, 0, len(want))
	for _, s := range want {
		if _, ok := seen[s]; !ok {
			notSeen = append(notSeen, s)
		}
	}
	if len(notSeen) != 0 {
		t.Errorf("not seen: %v", notSeen)
	}
}
