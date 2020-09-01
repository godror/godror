// Copyright 2019, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func TestQueue(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("Queue"), 30*time.Second)
	defer cancel()
	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var user string
	if err = tx.QueryRowContext(ctx, "SELECT USER FROM DUAL").Scan(&user); err != nil {
		t.Fatal(err)
	}

	const qName = "TEST_Q"
	const qTblName = qName + "_TBL"
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
	if _, err = tx.ExecContext(ctx, qry); err != nil {
		if strings.Contains(err.Error(), "PLS-00201: identifier 'SYS.DBMS_AQADM' must be declared") {
			t.Skip(err.Error())
		}
		t.Log(fmt.Errorf("%s: %w", qry, err))
	}
	defer func() {
		testDb.ExecContext(
			testContext("Queue-drop"),
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
	}()

	q, err := godror.NewQueue(ctx, tx, qName, "", godror.WithEnqOptions(godror.EnqOptions{
		Visibility:   godror.VisibleOnCommit,
		DeliveryMode: godror.DeliverPersistent,
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	t.Logf("name=%q q=%#v", q.Name(), q)
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
	msgs := make([]godror.Message, 2)
	const msgCount = 2 * maxSessions
	want := make([]string, 0, msgCount)
	for i := 0; i < msgCount; {
		for j := range msgs {
			want = append(want, fmt.Sprintf("%03d. árvíztűrő tükörfúrógép", i))
			msgs[j] = godror.Message{
				Expiration: 10 * time.Second,
				Raw:        []byte(want[len(want)-1]),
			}
			i++
		}
		if err = q.Enqueue(msgs); err != nil {
			var ec interface{ Code() int }
			if errors.As(err, &ec) && ec.Code() == 24444 {
				t.Skip(err)
			}
			t.Fatal("enqueue:", err)
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
	{
		const qry = `SELECT COUNT(0) FROM ` + qTblName
		var n int32
		if err := testDb.QueryRowContext(ctx, qry).Scan(&n); err != nil {
			t.Fatalf("%q: %+v", qry, err)
		}
		t.Logf("%d rows in %s", n, qTblName)
	}

	seen := make(map[string]int, msgCount)
	msgs = msgs[:cap(msgs)]
	for i := 0; i < msgCount; i++ {
		if func(i int) int {
			tx, err := testDb.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			// Let's test deqOne
			if i == msgCount/3 {
				msgs = msgs[:1]
			}
			q, err := godror.NewQueue(ctx, tx, qName, "",
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
			{
				const qry = `SELECT COUNT(0) FROM ` + qTblName
				var n int32
				if err := testDb.QueryRowContext(ctx, qry).Scan(&n); err != nil {
					t.Fatalf("%q: %+v", qry, err)
				}
				t.Logf("%d rows in %s", n, qTblName)
			}
			if n == 0 {
				return 0
			}
			for j, m := range msgs[:n] {
				if len(m.Raw) == 0 {
					t.Logf("%d/%d. received empty message: %#v", i, j, m)
					continue
				}
				s := string(m.Raw)
				t.Logf("%d/%d: got: %q", i, j, s)
				if k, ok := seen[s]; ok {
					t.Fatalf("%d. %q already seen in %d", i, s, k)
				}
				seen[s] = i
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			return n
		}(i) == 0 {
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

func TestQueueObject(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("QueueObject"), 30*time.Second)
	defer cancel()

	var user string
	if err := testDb.QueryRowContext(ctx, "SELECT USER FROM DUAL").Scan(&user); err != nil {
		t.Fatal(err)
	}

	const qName = "TEST_QOBJ"
	const qTblName = qName + "_TBL"
	const qTypName = qName + "_TYP"
	const arrTypName = qName + "_ARR_TYP"
	testDb.ExecContext(ctx, "DROP TYPE "+qTypName)
	testDb.ExecContext(ctx, "DROP TYPE "+arrTypName)

	var plus strings.Builder
	for _, qry := range []string{
		"BEGIN SYS.DBMS_AQADM.stop_queue('" + user + "." + qName + "'); END;",
		"BEGIN SYS.DBMS_AQADM.drop_queue('" + user + "." + qName + "'); END;",
		"BEGIN SYS.DBMS_AQADM.drop_queue_table('" + user + "." + qTblName + "'); END;",
		"DROP TABLE " + user + "." + qTblName,
		"DROP TYPE " + user + "." + qTypName + " FORCE",
		"DROP TYPE " + user + "." + arrTypName + " FORCE",
		"CREATE OR REPLACE TYPE " + user + "." + arrTypName + " IS TABLE OF VARCHAR2(1000)",
		"CREATE OR REPLACE TYPE " + user + "." + qTypName + " IS OBJECT (f_vc20 VARCHAR2(20), f_num NUMBER, f_dt DATE/*, f_arr " + arrTypName + "*/)",
	} {
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			if strings.HasPrefix(qry, "CREATE ") || !strings.Contains(err.Error(), "not exist") {
				t.Log(fmt.Errorf("%s: %w", qry, err))
			}
			if strings.Contains(err.Error(), "PLS-00201: identifier 'SYS.DBMS_AQADM' must be declared") {
				t.Skip(err)
			}
			continue
		}
		plus.WriteString(qry)
		plus.WriteString(";\n")
	}
	defer func() {
		testDb.Exec("DROP TYPE " + user + "." + qTypName)
		testDb.Exec("DROP TYPE " + user + "." + arrTypName)
	}()
	{
		qry := `DECLARE
		tbl CONSTANT VARCHAR2(61) := '` + user + "." + qTblName + `';
		q CONSTANT VARCHAR2(61) := '` + user + "." + qName + `';
		typ CONSTANT VARCHAR2(61) := '` + user + "." + qTypName + `';
	BEGIN
		BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
		BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
		BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl); EXCEPTION WHEN OTHERS THEN NULL; END;

		SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, typ);
		SYS.DBMS_AQADM.CREATE_QUEUE(q, tbl);

		SYS.DBMS_AQADM.grant_queue_privilege('ENQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.grant_queue_privilege('DEQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.start_queue(q);
	END;`
		if _, err := testDb.ExecContext(ctx, qry); err != nil {
			t.Logf("%v", fmt.Errorf("%s: %w", qry, err))
		}
	}
	defer func() {
		if _, err := testDb.ExecContext(
			testContext("QueueObject-drop"),
			`DECLARE
			tbl CONSTANT VARCHAR2(61) := USER||'.'||:1;
			q CONSTANT VARCHAR2(61) := USER||'.'||:2;
		BEGIN
			BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl); EXCEPTION WHEN OTHERS THEN NULL; END;
		END;`,
			qTblName, qName,
		); err != nil {
			t.Log(err)
		}
	}()

	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	q, err := godror.NewQueue(ctx, tx, qName, qTypName, godror.WithEnqOptions(godror.EnqOptions{
		Visibility:   godror.VisibleOnCommit,
		DeliveryMode: godror.DeliverPersistent,
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	t.Log("name:", q.Name())
	enqOpts, err := q.EnqOptions()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("enqOpts: %#v", enqOpts)

	oTyp, err := godror.GetObjectType(ctx, tx, qTypName)
	if err != nil {
		t.Fatal(err)
	}
	defer oTyp.Close()
	var data godror.Data

	// Put some messages into the queue
	msgs := make([]godror.Message, 2)
	const msgCount = 2 * maxSessions
	want := make([]int, 0, msgCount)
	for i := 0; i < msgCount; {
		for j := range msgs {
			obj, err := oTyp.NewObject()
			if err != nil {
				t.Fatalf("%d. %+v", i, err)
			}
			defer obj.Close()
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
			want = append(want, k)
			i++
			msgs[j].Object = obj
			msgs[j].Expiration = 10 * time.Second
		}
		if err = q.Enqueue(msgs); err != nil {
			var ec interface{ Code() int }
			if errors.As(err, &ec) && ec.Code() == 24444 {
				t.Skip(err)
			}
			t.Fatal("enqueue:", err)
		}
		if i >= msgCount/3 {
			msgs = msgs[:1]
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	q.Close()
	oTyp.Close()

	seen := make(map[int]int, msgCount)
	msgs = msgs[:cap(msgs)]
	for i := 0; i < msgCount; i++ {
		if func(i int) int {
			tx, err := testDb.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			// Let's test deqOne
			if i == msgCount/3 {
				msgs = msgs[:1]
			}
			q, err := godror.NewQueue(ctx, tx, qName, qTypName,
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
				if m.Object == nil {
					t.Logf("%d/%d. received empty message: %#v", i, j, m)
					continue
				}
				if err = m.Object.GetAttribute(&data, "F_NUM"); err != nil {
					t.Fatal(err)
				}
				m.Object.Close()
				s := int(data.GetFloat64())
				//defer m.Object.ObjectType.Close()
				t.Logf("%d/%d: got: %q", i, j, s)
				if k, ok := seen[s]; ok {
					t.Fatalf("%d. %q already seen in %d", i, s, k)
				}
				seen[s] = i
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			return n
		}(i) == 0 {
			break
		}
	}

	PrintConnStats()

	t.Logf("seen: %v", seen)
	notSeen := make([]int, 0, len(want))
	for _, s := range want {
		if _, ok := seen[s]; !ok {
			notSeen = append(notSeen, s)
		}
	}
	if len(notSeen) != 0 {
		t.Errorf("not seen: %v", notSeen)
	}
}
