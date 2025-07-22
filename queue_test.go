// Copyright 2019, 2025 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
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

func TestQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("Queue"), 30*time.Second)
	defer cancel()

	t.Run("deqbymsgid", func(t *testing.T) {
		const qName = "TEST_MSGID_Q"
		const qTblName = qName + "_TBL"
		setUp := func(ctx context.Context, db execer, user string) error {
			qry := `DECLARE
		tbl CONSTANT VARCHAR2(61) := '` + user + "." + qTblName + `';
		q CONSTANT VARCHAR2(61) := '` + user + "." + qName + `';
	BEGIN
		BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
		BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
		BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;

		SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, 'RAW');
		SYS.DBMS_AQADM.CREATE_QUEUE(q, tbl);
		SYS.DBMS_AQADM.grant_queue_privilege('ENQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.grant_queue_privilege('DEQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.start_queue(q);
	END;`
			_, err := db.ExecContext(ctx, qry)
			return err
		}

		tearDown := func(ctx context.Context, db execer, user string) error {
			db.ExecContext(
				ctx,
				`DECLARE
			tbl CONSTANT VARCHAR2(61) := USER||'.'||:1;
			q CONSTANT VARCHAR2(61) := USER||'.'||:2;
		BEGIN
			BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;
		END;`,
				qTblName, qName,
			)
			return nil
		}

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

		t.Log("deqbymsgid")
		if err = func() error {
			q, err := godror.NewQueue(ctx, tx, qName, "")
			t.Log("q:", q, "err:", err)
			if err != nil {
				return err
			}
			defer q.Close()

			msgs := make([]godror.Message, 1)
			msgs[0] = godror.Message{Raw: []byte("msg to be dequeued")}
			msgs[0].Expiration = 60 * time.Second

			if err = q.Enqueue(msgs); err != nil {
				var ec interface{ Code() int }
				if errors.As(err, &ec) && ec.Code() == 24444 {
					t.Skip(err)
				}
				return err
			}
			if err = tx.Commit(); err != nil {
				return err
			}

			b := msgs[0].MsgID[:]

			tx, err := testDb.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()

			opts, err := q.DeqOptions()
			if err != nil {
				return err
			}

			opts.Mode = godror.DeqRemove
			opts.MsgID = b
			opts.Wait = 1 * time.Second
			t.Logf("opts: %#v", opts)

			n, err := q.DequeueWithOptions(msgs[:1], &opts)
			if err != nil || n == 0 {
				return fmt.Errorf("dequeue by msgid: %d/%+v", n, err)
			}

			if err = tx.Commit(); err != nil {
				return err
			}

			if !bytes.Equal(msgs[0].MsgID[:], b) {
				return fmt.Errorf("set %v, got %v as msgs[0].MsgID", b, msgs[0].MsgID)
			}

			return nil
		}(); err != nil {
			t.Error(err)
		}
	})

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
		BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;

		SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, 'RAW');
		SYS.DBMS_AQADM.CREATE_QUEUE(q, tbl);
		SYS.DBMS_AQADM.grant_queue_privilege('ENQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.grant_queue_privilege('DEQUEUE', q, '` + user + `');
		--SYS.DBMS_AQADM.start_queue(q);
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
			BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;
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
		BEGIN SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, typ); EXCEPTION WHEN OTHERS THEN IF SQLCODE <> -24001 THEN RAISE; END IF; END;
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
			BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;
		END;`
				if _, err := db.ExecContext(ctx, qry, qTblName, qName); err != nil {
					t.Logf("%q: %+v", qry, err)
				}
				for _, qry := range []string{
					"BEGIN SYS.DBMS_AQADM.stop_queue(" + user + "." + qName + "); END;",
					"BEGIN SYS.DBMS_AQADM.drop_queue(" + user + "." + qName + "); END;",
					"BEGIN SYS.DBMS_AQADM.drop_queue_table(" + user + "." + qTblName + ", TRUE); END;",
					"DROP TABLE " + user + "." + qTblName,
					"DROP TYPE " + user + "." + qTypName + " FORCE",
					"DROP TYPE " + user + "." + arrTypName + " FORCE",
				} {
					if _, err := db.ExecContext(ctx, qry); err != nil {
						t.Logf("%q: %+v", qry, err)
					}
				}
				return nil
			},

			func(q *godror.Queue, i int) (godror.Message, string) {
				obj, err := q.PayloadObjectType.NewObject()
				if err != nil {
					t.Fatalf("%d. %+v", i, err)
				}
				if err = obj.Set("F_DT", time.Now()); err != nil {
					t.Fatal(err)
				}
				if err = obj.Set("F_VC20", "árvíztűrő"); err != nil {
					t.Fatal(err)
				}

				if err = obj.Set("F_NUM", int64(i)); err != nil {
					t.Fatal(err)
				}
				if err = obj.GetAttribute(&data, "F_NUM"); err != nil {
					t.Fatal(err)
				}
				num := string(data.GetBytes())
				k, err := strconv.ParseInt(num, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				if k != int64(i) {
					t.Fatalf("F_NUM as float got %d, wanted %d (have %#v (ntt=%d))", k, i, data.Get(), data.NativeTypeNum)
				}
				var buf bytes.Buffer
				if err := obj.ToJSON(&buf); err != nil {
					t.Error(err)
				}
				t.Logf("obj=%s; %q", buf.String(), num)
				var x struct {
					Num string `json:"F_NUM"`
				}
				if err := json.Unmarshal(buf.Bytes(), &x); err != nil {
					t.Fatal(err)
				}
				if x.Num != strconv.Itoa(i) {
					t.Errorf("ToJSON says %q (%s), wanted %d", x.Num, buf.String(), i)
				}
				return godror.Message{Object: obj}, num
			},

			func(m godror.Message, i int) (string, error) {
				var data godror.Data
				if m.Object == nil {
					t.Logf("%d. received empty message: %#v", i, m)
					return "", nil
				}
				defer m.Object.Close() // NOT before data use!
				var buf bytes.Buffer
				if err := m.Object.ToJSON(&buf); err != nil {
					t.Error(err)
				}
				t.Logf("obj=%s", buf.String())
				attr := m.Object.Attributes["F_NUM"]
				if err := m.Object.GetAttribute(&data, attr.Name); err != nil {
					return "", err
				}
				v := data.GetBytes()
				s := string(v)
				t.Logf("cm %d: got F_NUM=%q (%T ntn=%d otn=%d)", i, s, v, data.NativeTypeNum, attr.ObjectType.OracleTypeNum)
				var x struct {
					Num string `json:"F_NUM"`
				}
				if err := json.Unmarshal(buf.Bytes(), &x); err != nil {
					t.Error(err)
				} else if x.Num != s {
					t.Errorf("json=%q (%s), wanted %q", x.Num, buf.String(), s)
				}
				return s, nil
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
	var user string
	if err := testDb.QueryRowContext(ctx, "SELECT USER FROM DUAL").Scan(&user); err != nil {
		t.Fatal(err)
	}

	if err := tearDown(ctx, testDb, user); err != nil {
		t.Log("tearDown:", err)
	}
	if err := setUp(ctx, testDb, user); err != nil {
		if strings.Contains(err.Error(), "PLS-00201: identifier 'SYS.DBMS_AQADM' must be declared") {
			t.Skip(err.Error())
		}
		t.Fatalf("setUp: %+v", err)
	}
	defer func() {
		if err := tearDown(testContext("queue-teardown"), testDb, user); err != nil {
			t.Log("tearDown:", err)
		}
	}()

	msgCount := 3 * maxSessions
	want := make([]string, 0, msgCount)
	seen := make(map[string]int, msgCount)
	msgs := make([]godror.Message, maxSessions)

	if err := func() error {
		tx, err := testDb.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		q, err := godror.NewQueue(ctx, tx, qName, objName,
			godror.WithEnqOptions(godror.EnqOptions{
				Visibility:   godror.VisibleOnCommit,
				DeliveryMode: godror.DeliverPersistent,
			}),
		)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		defer q.Close()

		t.Logf("name=%q obj=%q q=%#v", q.Name(), objName, q)
		start := time.Now()
		if err = q.PurgeExpired(ctx); err != nil {
			return fmt.Errorf("%q.PurgeExpired: %w", q.Name(), err)
		}
		t.Logf("purge dur=%s", time.Since(start))
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

		// Put some messages into the queue
		start = time.Now()
		for i := 0; i < msgCount; {
			// Let's test enqOne
			if msgCount-i < 3 {
				msgs = msgs[:1]
			}
			for j := range msgs {
				var s string
				msgs[j], s = newMessage(q, i)
				msgs[j].Expiration = 30 * time.Second
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
		}
		t.Logf("enqueued %d messages dur=%s", msgCount, time.Since(start))
		return tx.Commit()
	}(); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	msgs = msgs[:cap(msgs)]
	for i := 0; i < msgCount; {
		z := 2
		n := func(i int) int {
			tx, err := testDb.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			q, err := godror.NewQueue(ctx, tx, qName, objName,
				godror.WithDeqOptions(godror.DeqOptions{
					Mode:       godror.DeqRemove,
					Visibility: godror.VisibleOnCommit,
					Navigation: godror.NavNext,
					Wait:       10 * time.Second,
				}))
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()

			// stop queue to test auto-starting it
			if i == msgCount-1 {
				const qry = `BEGIN DBMS_AQADM.stop_queue(queue_name=>:1); END;`
				if _, err := tx.ExecContext(ctx, qry, q.Name()); err != nil {
					t.Log(qry, err)
				}

				// Let's test deqOne
				msgs = msgs[:1]
			}

			//t.Logf("name=%q q=%#v", q.Name(), q)
			n, err := q.Dequeue(msgs)
			t.Logf("%d. received %d message(s)", i, n)
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

			if err := q.PurgeExpired(ctx); err != nil && !errors.Is(err, driver.ErrBadConn) {
				t.Errorf("%q.PurgeExpired: %+v", q.Name(), err)
			}

			//i += n
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			return n
		}(i)
		i += n
		if n == 0 {
			z--
			if z == 0 {
				break
			}
			time.Sleep(time.Second)
		}
	}
	t.Logf("retrieved %d messages dur=%s", len(seen), time.Since(start))

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
