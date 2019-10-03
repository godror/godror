// Copyright 2019 Tamás Gulácsi
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
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	errors "golang.org/x/xerrors"

	goracle "gopkg.in/goracle.v2"
)

func TestQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var user string
	if err = conn.QueryRowContext(ctx, "SELECT USER FROM DUAL").Scan(&user); err != nil {
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
	if _, err = conn.ExecContext(ctx, qry); err != nil {
		t.Log(errors.Errorf("%s: %w", qry, err))
	}
	defer func() {
		conn.ExecContext(
			context.Background(),
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

	q, err := goracle.NewQueue(ctx, conn, qName, "")
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
	deqOpts, err := q.DeqOptions()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("deqOpts: %#v", deqOpts)

	if err = q.Enqueue([]goracle.Message{goracle.Message{Raw: []byte("árvíztűrő tükörfúrógép")}}); err != nil {
		var ec interface{ Code() int }
		if errors.As(err, &ec) && ec.Code() == 24444 {
			t.Skip(err)
		}
		t.Fatal("enqueue:", err)
	}
	msgs := make([]goracle.Message, 1)
	n, err := q.Dequeue(msgs)
	if err != nil {
		t.Error("dequeue:", err)
	}
	t.Logf("received %d messages", n)
	for _, m := range msgs[:n] {
		t.Logf("got: %#v (%q)", m, string(m.Raw))
	}
}
func TestQueueObject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var user string
	if err = conn.QueryRowContext(ctx, "SELECT USER FROM DUAL").Scan(&user); err != nil {
		t.Fatal(err)
	}

	const qName = "TEST_QOBJ"
	const qTblName = qName + "_TBL"
	const qTypName = qName + "_TYP"
	const arrTypName = qName + "_ARR_TYP"
	conn.ExecContext(ctx, "DROP TYPE "+qTypName)
	conn.ExecContext(ctx, "DROP TYPE "+arrTypName)

	var plus strings.Builder
	for _, qry := range []string{
		"CREATE OR REPLACE TYPE " + user + "." + arrTypName + " IS TABLE OF VARCHAR2(1000)",
		"CREATE OR REPLACE TYPE " + user + "." + qTypName + " IS OBJECT (f_vc20 VARCHAR2(20), f_num NUMBER, f_dt DATE/*, f_arr " + arrTypName + "*/)",
	} {
		if _, err = conn.ExecContext(ctx, qry); err != nil {
			t.Log(errors.Errorf("%s: %w", qry, err))
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
		if _, err = conn.ExecContext(ctx, qry); err != nil {
			t.Log(errors.Errorf("%s\n%s:%w", plus.String(), qry, err))
		}
	}
	defer func() {
		conn.ExecContext(
			context.Background(),
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

	q, err := goracle.NewQueue(ctx, conn, qName, qTypName)
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
	deqOpts, err := q.DeqOptions()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("deqOpts: %#v", deqOpts)

	oTyp, err := goracle.GetObjectType(ctx, conn, qTypName)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := oTyp.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	if err = obj.Set("F_DT", time.Now()); err != nil {
		t.Error(err)
	}
	if err = obj.Set("F_VC20", "árvíztűrő"); err != nil {
		t.Error(err)
	}
	if err = obj.Set("F_NUM", 3.14); err != nil {
		t.Error(err)
	}
	t.Log("obj:", obj)
	if err = q.Enqueue([]goracle.Message{goracle.Message{Object: obj}}); err != nil {
		var ec interface{ Code() int }
		if errors.As(err, &ec) && ec.Code() == 24444 {
			t.Skip(err)
		}
		t.Fatal("enqueue:", err)
	}
	msgs := make([]goracle.Message, 1)
	n, err := q.Dequeue(msgs)
	if err != nil {
		t.Error("dequeue:", err)
	}
	t.Logf("received %d messages", n)
	for _, m := range msgs[:n] {
		t.Logf("got: %#v (%q)", m, string(m.Raw))
	}
}

func TestWscQueue(t *testing.T) {
	gCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var queueName, queueTypeName string
	{
		shortCtx, shortCancel := context.WithTimeout(gCtx, time.Second)
		_, err := testDb.ExecContext(shortCtx, `BEGIN
	:1 := DB_wsc.c_queue_name;
	:2 := DB_wsc.c_queue_type_name;
		END;`, sql.Out{Dest: &queueName}, sql.Out{Dest: &queueTypeName},
		)
		shortCancel()
		if err != nil {
			t.Skip(err)
		}
	}
	for _, nm := range []string{"REQ", "RESP"} {
		nm := nm
		t.Run(nm, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			cx, err := testDb.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer cx.Close()
			Q, err := goracle.NewQueue(ctx, cx, queueName+"_"+nm, queueTypeName+"_"+nm)
			if err != nil {
				t.Skip(err)
			}
			defer Q.Close()
			t.Log("Q:", Q.Name())
			eOpts, err := Q.EnqOptions()
			if err != nil {
				t.Fatal(err)
			}
			eOpts.DeliveryMode = goracle.DeliverPersistent
			eOpts.Visibility = goracle.VisibleImmediate
			t.Logf("eOpts=%+v", eOpts)
			if err = Q.SetEnqOptions(eOpts); err != nil {
				t.Fatal(err)
			}

			obj, err := Q.PayloadObjectType.NewObject()
			if err != nil {
				t.Fatal(err)
			}
			defer obj.Close()
			if nm == "REQ" {
				if err = obj.Set("FUNC", "EXIT"); err != nil {
					t.Error(err)
				}
			} else {
				if err = obj.Set("PAYLOAD", []byte("árvíztűrő tükörfúrógép")); err != nil {
					t.Error(err)
				}
			}
			msg := goracle.Message{Correlation: nm, Object: obj, Expiration: 30}
			t.Logf("%s sends %+v", nm, msg)
			if err = Q.Enqueue([]goracle.Message{msg}); err != nil {
				t.Fatal(err)
			}

			dOpts, err := Q.DeqOptions()
			if err != nil {
				t.Fatal(err)
			}
			dOpts.Correlation = nm
			dOpts.DeliveryMode = goracle.DeliverPersistentOrBuffered
			dOpts.Mode = goracle.DeqRemove
			dOpts.Visibility = goracle.VisibleImmediate
			if d, ok := ctx.Deadline(); ok {
				dOpts.Wait = uint32(time.Until(d) / time.Second)
			}
			t.Logf("dOpts=%+v", dOpts)
			if err = Q.SetDeqOptions(dOpts); err != nil {
				t.Fatal(err)
			}
			msgs := make([]goracle.Message, 1)
			n, err := Q.Dequeue(msgs)
			t.Log("n:", n, "msgs", msgs[:n], "error", err)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				t.Fatal("wanted 1 message, got 1")
			}
			t.Logf("%s received %+v", nm, msgs[0])
		})
	}
}
