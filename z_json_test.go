package godror_test

import (
	"bytes"
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/godror/godror"
)

func TestReadWriteEJSON(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("ReadWriteEJSON"), 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tbl := "test_personcollection_ejson" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	_, err = conn.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (id NUMBER(6), jdoc JSON)", //nolint:gas
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf(" JSON Document table  %q): ", tbl)

	defer testDb.Exec(
		"DROP TABLE " + tbl, //nolint:gas
	)

	stmt, err := conn.PrepareContext(ctx,
		"INSERT INTO "+tbl+" (id, jdoc) VALUES (:1, :2)", //nolint:gas
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for tN, tC := range []struct {
		JDOC   string
		Wanted string
	}{
		{JDOC: "{\"person\":{\"Name\":\"Alex\",\"BirthDate\":{\"$oracleDate\":\"1999-02-03T00:00:00\"},\"Experience\":{\"$intervalYearMonth\" : \"P10Y8M\"},\"ID\":{\"$numberInt\" :12},\"JoinDate\":{\"$oracleTimestampTZ\": \"2020-11-24T12:34:56.123000Z\"},\"age\":25,\"creditScore\":[700,250,340],\"salary\":{\"$numberFloat\":4500.23}}}", Wanted: "{\"person\":{\"Name\":\"Alex\",\"BirthDate\":\"1999-02-03T00:00:00\",\"Experience\":\"P10Y8M\",\"ID\":12,\"JoinDate\":\"2020-11-24T12:34:56.123000Z\",\"age\":25,\"creditScore\":[700,250,340],\"salary\":4500.23}}"},
		{JDOC: "{\"person\":{\"Name\":\"John\",\"BirthDate\":{\"$oracleDate\":\"1999-02-03T00:00:00\"},\"Experience\":{\"$intervalYearMonth\" : \"P10Y8M\"},\"ID\":{\"$numberInt\" :12},\"JoinDate\":{\"$oracleTimestampTZ\": \"2020-11-24T12:34:56.123000Z\"},\"age\":25,\"creditScore\":[800,250,340],\"salary\":{\"$numberFloat\":4500.23}}}", Wanted: "{\"person\":{\"Name\":\"John\",\"BirthDate\":\"1999-02-03T00:00:00\",\"Experience\":\"P10Y8M\",\"ID\":12,\"JoinDate\":\"2020-11-24T12:34:56.123000Z\",\"age\":25,\"creditScore\":[800,250,340],\"salary\":4500.23}}"},
	} {
		jsonval, _ := godror.NewJSONValue(tC.JDOC)

		if _, err = stmt.ExecContext(ctx, tN*2, jsonval); err != nil {
			t.Errorf("%d/1. (%v): %v", tN, tC.JDOC, err)
			continue
		}

		var rows *sql.Rows
		rows, err = conn.QueryContext(ctx,
			"SELECT id, jdoc FROM "+tbl+" where id = :1", tN*2) //nolint:gas
		if err != nil {
			t.Errorf("%d/3. %v", tN, err)
			continue
		}
		for rows.Next() {
			var id, jsondoc interface{}
			if err = rows.Scan(&id, &jsondoc); err != nil {
				rows.Close()
				t.Errorf("%d/3. scan: %v", tN, err)
				continue
			}
			if jsondoc, ok := jsondoc.(godror.JSON); !ok {
				t.Errorf("%d. %T is not json Doc", id, jsondoc)
			} else {
				t.Logf("%d. JSON Document read %q): ", id, jsondoc)
				got := jsondoc.String()
				if err != nil {
					t.Errorf("%d. %v", id, err)
				} else if !bytes.Equal([]byte(got), []byte(tC.Wanted)) {
					t.Errorf("%d. got %q for JDOC, wanted %q", id, got, tC.Wanted)
				}
			}
		}
		rows.Close()
	}
}

func TestReadWriteJSONMap(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("ReadWriteEJSON"), 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tbl := "test_personcollection_jsonmap" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	_, err = conn.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (id NUMBER(6), jdoc JSON)", //nolint:gas
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf(" JSON Document table  %q): ", tbl)

	defer testDb.Exec(
		"DROP TABLE " + tbl, //nolint:gas
	)

	stmt, err := conn.PrepareContext(ctx,
		"INSERT INTO "+tbl+" (id, jdoc) VALUES (:1, :2)", //nolint:gas
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	jmap := map[string]interface{}{
		"person": map[string]interface{}{
			"ID":        12,
			"FirstName": "john",
			"creditScore": []interface{}{
				700,
				250,
				340,
			},
			"age":    25,
			"salary": 4500.23,
		},
	}
	for tN, tC := range []struct {
		JDOC map[string]interface{}
	}{
		{JDOC: jmap},
	} {
		jsonval, _ := godror.NewJSONValue(tC.JDOC)
		if _, ok := jsonval.(godror.JSONObject); !ok {
			t.Errorf("%d Casting to JSONObject Failed", tN)
		}

		if _, err = stmt.ExecContext(ctx, tN*2, jsonval); err != nil {
			t.Errorf("%d/1. (%v): %v", tN, tC.JDOC, err)
			continue
		}

		var rows *sql.Rows
		rows, err = conn.QueryContext(ctx,
			"SELECT id, jdoc FROM "+tbl) //nolint:gas
		if err != nil {
			t.Errorf("%d/3. %v", tN, err)
			continue
		}
		for rows.Next() {
			var id, jsondoc interface{}
			if err = rows.Scan(&id, &jsondoc); err != nil {
				rows.Close()
				t.Errorf("%d/3. scan: %v", tN, err)
				continue
			}
			if jsondocJSON, ok := jsondoc.(godror.JSON); !ok {
				t.Errorf("%d. %T is not JSON Doc", id, jsondoc)
			} else {
				jsondocJSON.JSONOption = godror.JSONOptDefault
				t.Logf("%d. JSON Document read %q): ", id, jsondoc)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				} else {
					var jobj godror.JSONObject
					err = jsondocJSON.GetJSONObject(&jobj)
					if err != nil {
						t.Errorf("%d. %v", id, err)
					}
					wantmap := jobj.AsMap()
					got := wantmap["person"]
					var objmap = make(map[string]godror.Data)
					objmap = got.GetJSONObject().AsMap()
					gotAge := objmap["age"]
					wantage := 25
					if gotAge.Get() != float64(wantage) {
						t.Errorf("%d. got %v for JDOC, wanted %v", id, gotAge.Get(), wantage)
					}
				}
			}
		}
		rows.Close()
	}
}
