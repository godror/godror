// Copyright 2018, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"bytes"
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// Inserts the JSON Formatted string with extended types for OSON bytes and
// BSON type . Reads from DB as standard JSON( extended strings removed)
// and does byte compare.
// Without extended strings in JSON format, the extended data types like Date
// would get stored as strings.
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
		jsonval := godror.JSONString{Value: tC.JDOC, Flags: godror.JSONFormatExtnTypes | godror.JSONFormatBSONTypes | godror.JSONFormatBSONTypePattern}

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
				t.Errorf("%d. %T is not Json Doc", id, jsondoc)
			} else {
				t.Logf("%d. JSON Document read %q): ", id, jsondoc)
				got, err := jsondoc.ToJSONString(0)
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

// It inserts Go map[string]interface{} and reads the JSON Document from DB.
// converts JSON Document into map[string]interface{} and compares with source
func TestReadWriteJSONMap(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("ReadWriteJsonMap"), 30*time.Second)
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
	birthdate, err := time.Parse(time.UnixDate, "Wed Feb 25 11:06:39 PST 1990")
	jmap := map[string]interface{}{
		"person": map[string]interface{}{
			"ID":        float64(12),
			"FirstName": "Mary",
			"LastName":  "John",
			"creditScore": []interface{}{
				float64(700),
				float64(250),
				float64(340),
			},
			"age":       float64(25),
			"BirthDate": birthdate,
			"salary":    float64(4500.2351),
			"Local":     true,
		},
	}
	for tN, tC := range []struct {
		JDOC map[string]interface{}
	}{
		{JDOC: jmap},
	} {
		jsonval, _ := godror.NewJSONValue(tC.JDOC)
		var jsonobj godror.JSONObject
		var ok bool
		if jsonobj, ok = jsonval.(godror.JSONObject); !ok {
			t.Errorf("%d Casting to JsonObject Failed", tN)
		}
		defer jsonobj.Close()

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
		defer rows.Close()
		for rows.Next() {
			var id interface{}
			var jsondoc godror.JSONValue
			if err = rows.Scan(&id, &jsondoc); err != nil {
				t.Errorf("%d/3. scan: %v", tN, err)
				continue
			}
			if jsondocJSON, ok := jsondoc.(godror.JSON); !ok {
				t.Errorf("%d. %T is not Json Doc", id, jsondoc)
			} else {
				t.Logf("%d. JSON Document read %q): ", id, jsondoc)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				} else {
					var jobj godror.JSONObject
					err = jsondocJSON.GetJSONObject(&jobj, godror.JSONOptDefault)
					if err != nil {
						t.Errorf("%d. %v", id, err)
					}
					jsonmapobj, err := jobj.GetValue()
					if err != nil {
						t.Errorf("%d. %v", id, err)
					}
					eq := reflect.DeepEqual(tC.JDOC, jsonmapobj)
					if !eq {
						t.Errorf("Got %+v, wanted %+v", jsonmapobj, tC.JDOC)
					}
				}
			}
		}
	}
}

// It inserts Go Array []interface{} and reads the JSON Document from DB.
// converts JSON Document into []interface{} and compares with source
func TestReadWriteJSONArray(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("ReadWriteJsonArray"), 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tbl := "test_personcollection_jsonarr" + tblSuffix
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
	birthdate, err := time.Parse(time.UnixDate, "Wed Feb 25 11:06:39 PST 1990")
	//birthdate := time.Now()
	jsarray := []interface{}{
		map[string]interface{}{
			"person": map[string]interface{}{
				"ID":        float64(12),
				"FirstName": "Mary",
				"LastName":  "John",
				"creditScore": []interface{}{
					float64(700),
					float64(250),
					float64(340),
				},
				"age":       float64(25),
				"BirthDate": birthdate,
				"salary":    float64(4500.2351),
				"Local":     true,
			},
		},
		map[string]interface{}{
			"person": map[string]interface{}{
				"ID":        float64(13),
				"FirstName": "Ivan",
				"LastName":  "John",
				"creditScore": []interface{}{
					float64(800),
					float64(550),
					float64(340),
				},
				"age":       float64(22),
				"BirthDate": birthdate,
				"salary":    float64(4800.2351),
				"Local":     true,
			},
		},
		"nodata",
	}
	for tN, tC := range []struct {
		JDOC []interface{}
	}{
		{JDOC: jsarray},
	} {
		jsonval, _ := godror.NewJSONValue(tC.JDOC)
		var jsonarr godror.JSONArray
		var ok bool
		if jsonarr, ok = jsonval.(godror.JSONArray); !ok {
			t.Errorf("%d Casting to JsonArray Failed", tN)
		}
		defer jsonarr.Close()

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
		defer rows.Close()
		for rows.Next() {
			var id, jsondoc interface{}
			if err = rows.Scan(&id, &jsondoc); err != nil {
				t.Errorf("%d/3. scan: %v", tN, err)
				continue
			}
			if jsondocJSON, ok := jsondoc.(godror.JSON); !ok {
				t.Errorf("%d. %T is not Json Doc", id, jsondoc)
			} else {
				t.Logf("%d. JSON Document read %q): ", id, jsondoc)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				} else {
					var jarr godror.JSONArray
					err = jsondocJSON.GetJSONArray(&jarr, godror.JSONOptDefault)
					if err != nil {
						t.Errorf("%d. %v", id, err)
					}
					jsonarrobj, err := jarr.GetValue()
					if err != nil {
						t.Errorf("%d. %v", id, err)
					}
					eq := reflect.DeepEqual(tC.JDOC, jsonarrobj)
					t.Logf("%d. Got  Document read %+v): ", id, jsonarrobj)
					if !eq {
						t.Errorf("Got %+v, wanted %+v", jsonarrobj, tC.JDOC)
					}
				}
			}
		}
	}
}

// It fetches the JSON coloumn which is an array. Displays birthdates of
// each entry, person.
// It validates the birthdate matches with what is inserted.
func TestReadJSONScalar(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("ReadJsonScalar"), 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tbl := "test_personcollection_jsonarr" + tblSuffix
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
	birthdate, err := time.Parse(time.UnixDate, "Wed Feb 25 11:06:39 PST 1990")
	//birthdate := time.Now()
	jsarray := []interface{}{
		map[string]interface{}{
			"person": map[string]interface{}{
				"ID":        float64(12),
				"FirstName": "Mary",
				"LastName":  "John",
				"creditScore": []interface{}{
					float64(700),
					float64(250),
					float64(340),
				},
				"age":       float64(25),
				"BirthDate": birthdate,
				"salary":    float64(4500.2351),
				"Local":     true,
			},
		},
		map[string]interface{}{
			"person": map[string]interface{}{
				"ID":        float64(13),
				"FirstName": "Ivan",
				"LastName":  "John",
				"creditScore": []interface{}{
					float64(800),
					float64(550),
					float64(340),
				},
				"age":       float64(22),
				"BirthDate": birthdate,
				"salary":    float64(4800.2351),
				"Local":     true,
			},
		},
		"nodata",
	}
	for tN, tC := range []struct {
		JDOC []interface{}
	}{
		{JDOC: jsarray},
	} {
		jsonval, _ := godror.NewJSONValue(tC.JDOC)
		var jsonarr godror.JSONArray
		var ok bool
		if jsonarr, ok = jsonval.(godror.JSONArray); !ok {
			t.Errorf("%d Casting to JsonArray Failed", tN)
		}
		defer jsonarr.Close()

		if _, err = stmt.ExecContext(ctx, tN*2, jsonval); err != nil {
			t.Errorf("%d/1. (%v): %v", tN, tC.JDOC, err)
			continue
		}

		rows, err := conn.QueryContext(ctx,
			"SELECT id, c.jdoc.person, c.jdoc.person.BirthDate, json_value(jdoc,'$.person.type()'), json_value(jdoc,'$.person.BirthDate.type()')  FROM "+tbl+" c ") //nolint:gas
		if err != nil {
			t.Errorf("%d/3. %v", tN, err)
			continue
		}
		defer rows.Close()
		for rows.Next() {
			var id, person, personBirthDate interface{}
			var persontype, personDOBType string
			if err = rows.Scan(&id, &person, &personBirthDate, &persontype, &personDOBType); err != nil {
				t.Errorf("%d/3. scan: %v", tN, err)
				continue
			}

			// Get object person
			if personJSON, ok := person.(godror.JSON); !ok {
				t.Errorf("%d. %T is not Json Doc", id, person)
			} else {
				t.Logf("%d. JSON Document for Person %q: ", id, personJSON)
			}
			// Validate DB types for each
			wantDBtype := "object"
			if persontype != wantDBtype {
				t.Errorf("Got %+v, wanted %+v", persontype, wantDBtype)
			}
			wantDBtype = "timestamp"
			if personDOBType != wantDBtype {
				t.Errorf("Got %+v, wanted %+v", personDOBType, wantDBtype)
			}

			// Get scalar value Birthdate and verify
			if personBirthDateJSON, ok := personBirthDate.(godror.JSON); !ok {
				t.Errorf("%d. %T is not Json Doc", id, personBirthDate)
			} else {
				var birthDate godror.JSONScalar
				err = personBirthDateJSON.GetJSONScalar(&birthDate, godror.JSONOptDefault)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				}
				dob, err := birthDate.GetValue()
				if err != nil {
					t.Errorf("%d. %v", id, err)
				}
				if dobarr, ok := dob.(godror.JSONArray); !ok {
					t.Errorf("%d. BirthDate Array type is not %T", id, dobarr)
				} else {
					dobarrayval, err := dobarr.GetValue()
					if err != nil {
						t.Errorf("%d. %v", id, err)
					}
					for _, entry := range dobarrayval {
						if entry != birthdate {
							t.Errorf("Got %+v, wanted %+v", entry, birthdate)
						}
					}
				}
			}
		}
	}
}

// It retrieves the object , person from JSON coloumn and updates it.
func TestUpdateJSONObject(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("UpdateJSONObject"), 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tbl := "test_personcollection_jsonarr" + tblSuffix
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
	birthdate, _ := time.Parse(time.UnixDate, "Wed Feb 25 11:06:39 PST 1990")
	newBirthDate, _ := time.Parse(time.UnixDate, "Wed Feb 25 11:06:39 PST 1989")
	//birthdate := time.Now()
	jsarray := []interface{}{
		map[string]interface{}{
			"person": map[string]interface{}{
				"ID":        float64(12),
				"FirstName": "Mary",
				"LastName":  "John",
				"creditScore": []interface{}{
					float64(700),
					float64(250),
					float64(340),
				},
				"age":       float64(25),
				"BirthDate": birthdate,
				"salary":    float64(4500.2351),
				"Local":     true,
			},
		},
		"nodata",
	}

	for tN, tC := range []struct {
		JDOC []interface{}
	}{
		{JDOC: jsarray},
	} {
		jsonval, _ := godror.NewJSONValue(tC.JDOC)
		var jsonarr godror.JSONArray
		var ok bool
		if jsonarr, ok = jsonval.(godror.JSONArray); !ok {
			t.Errorf("%d Casting to JsonArray Failed", tN)
		}
		defer jsonarr.Close()

		if _, err = stmt.ExecContext(ctx, tN*2, jsonval); err != nil {
			t.Errorf("%d/1. (%v): %v", tN, tC.JDOC, err)
			continue
		}

		rows, err := conn.QueryContext(ctx,
			"SELECT id, c.jdoc.person FROM "+tbl+" c ") //nolint:gas
		if err != nil {
			t.Errorf("%d/3. %v", tN, err)
			continue
		}
		defer rows.Close()
		for rows.Next() {
			var id, person interface{}
			if err = rows.Scan(&id, &person); err != nil {
				t.Errorf("%d/3. scan: %v", tN, err)
				continue
			}

			// Get object person
			var personObj godror.JSONObject
			var personJSON godror.JSON
			if personJSON, ok = person.(godror.JSON); !ok {
				t.Errorf("%d. %T is not Json Doc", id, person)
			} else {
				t.Logf("%d. JSON Document for Person %q: ", id, personJSON)
			}
			err = personJSON.GetJSONObject(&personObj, godror.JSONOptDefault)
			if err != nil {
				t.Errorf("%d. %v", id, err)
			}

			personMap, err := personObj.GetValue()
			if err != nil {
				t.Errorf("%d. %v", id, err)
			}
			personMap["BirthDate"] = newBirthDate
			jsonval, _ := godror.NewJSONValue(personMap)
			var newPersonObj godror.JSONObject
			if newPersonObj, ok = jsonval.(godror.JSONObject); !ok {
				t.Errorf("%d Casting to JsonObject Failed", tN)
			}
			defer newPersonObj.Close()
			qry := "update " + tbl + " c set c.jdoc=JSON_TRANSFORM(c.jdoc, set '$.person'=:1)"
			_, err = conn.ExecContext(ctx, qry, jsonval)
			if err != nil {
				t.Errorf("%d. %v", id, err)
			}
		}
	}
}
