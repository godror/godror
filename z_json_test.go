// Copyright 2018, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// Inserts the JSON Formatted string without extended types.
// Reads from DB as standard JSON and compare with input JSON string.
// The Dates below are not stored as Oracle extended type, timestamp,
// instead they are stored as string.
// We need to use eJSON to retain types from JSON string.
// Alternatively if application uses map, array ,
// the extended types can be retained as show in tests, TestReadWriteJSONMap
// and TestReadWriteJSONArray.
func TestReadWriteJSONString(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("ReadWriteJSONString"), 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tbl := "test_personcollection_jsonstring" + tblSuffix
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
		{JDOC: "{\"person\":{\"Name\":\"Alex\",\"BirthDate\":\"1999-02-03T00:00:00\",\"ID\":12,\"JoinDate\":\"2020-11-24T12:34:56.123000Z\",\"age\":25,\"creditScore\":[700,250,340],\"salary\":45.23}}", Wanted: "{\"person\":{\"Name\":\"Alex\",\"BirthDate\":\"1999-02-03T00:00:00\",\"ID\":12,\"JoinDate\":\"2020-11-24T12:34:56.123000Z\",\"age\":25,\"creditScore\":[700,250,340],\"salary\":45.23}}"},
		{JDOC: "{\"person\":{\"Name\":\"John\",\"BirthDate\":\"1999-02-03T00:00:00\",\"ID\":12,\"JoinDate\":\"2020-11-24T12:34:56.123000Z\",\"age\":25,\"creditScore\":[800,250,340],\"salary\":4500.2351}}", Wanted: "{\"person\":{\"Name\":\"John\",\"BirthDate\":\"1999-02-03T00:00:00\",\"ID\":12,\"JoinDate\":\"2020-11-24T12:34:56.123000Z\",\"age\":25,\"creditScore\":[800,250,340],\"salary\":4500.2351}}"},
	} {
		jsonval := godror.JSONString{Value: tC.JDOC, Flags: 0}

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
		var id interface{}
		var jsondoc godror.JSON
		for rows.Next() {
			if err = rows.Scan(&id, &jsondoc); err != nil {
				rows.Close()
				t.Errorf("%d/3. scan: %v", tN, err)
				continue
			}
			t.Logf("%d. JSON Document read %q): ", id, jsondoc)
			got := jsondoc.String()
			if got == "" {
				t.Errorf("%d. %v", id, err)
			} else {
				eq, err := isEqualJSONString(got, tC.Wanted)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				}
				if !eq {
					t.Errorf("%d. got %q for JDOC, wanted %q", id, got, tC.Wanted)
				}
			}
		}
		rows.Close()
	}
}

// Check if two JSON strings are equal ignoring the order
func isEqualJSONString(js1, js2 string) (bool, error) {
	var js1type interface{}
	var js2type interface{}

	var err error
	err = json.Unmarshal([]byte(js1), &js1type)
	if err != nil {
		return false, err
	}
	err = json.Unmarshal([]byte(js2), &js2type)
	if err != nil {
		return false, err
	}
	return reflect.DeepEqual(js1type, js2type), nil

}

// It inserts Go map[string]interface{} and reads the JSON Document from DB.
// converts JSON Document into map[string]interface{}
// and compares with source.
// We are sending float64 types from map because from DB , we always
// get float64 for numbers used in JSON document.
// All int8, int16, int32, int64, float32, float64, uint8, uint16,
// uint32, uint64 are stored as Number in DB.
// Application can always convert to required types
// using conversions after fetching from DB.
// ex: toint8 = int8(val.float64)
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
			"salary":    float64(45.231),
			"Local":     true,
		},
	}
	for tN, tC := range []struct {
		JDOC map[string]interface{}
	}{
		{JDOC: jmap},
	} {
		jsonval, err := godror.NewJSONValue(tC.JDOC)
		if err != nil {
			t.Errorf("%d. %v", tN, err)
		}
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
		var id interface{}
		var jsondoc godror.JSON
		for rows.Next() {
			if err = rows.Scan(&id, &jsondoc); err != nil {
				rows.Close()
				t.Errorf("%d/3. scan: %v", tN, err)
				continue
			}

			t.Logf("%d. JSON Document read %q): ", id, jsondoc)
			if err != nil {
				t.Errorf("%d. %v", id, err)
			} else {
				var v interface{}
				var gotmap map[string]interface{}
				err = jsondoc.GetValue(godror.JSONOptNumberAsString, &v)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				}
				if gotmap, ok = v.(map[string]interface{}); !ok {
					t.Errorf("%d. %T is not JSONObject ", id, v)
				}
				eq := reflect.DeepEqual(tC.JDOC, gotmap)
				if !eq {
					t.Errorf("Got %+v, wanted %+v", gotmap, tC.JDOC)
				}
			}

		}
		rows.Close()
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
		var id interface{}
		var jsondoc godror.JSON
		for rows.Next() {
			if err = rows.Scan(&id, &jsondoc); err != nil {
				rows.Close()
				t.Errorf("%d/3. scan: %v", tN, err)
				continue
			}
			t.Logf("%d. JSON Document read %q): ", id, jsondoc)
			if err != nil {
				t.Errorf("%d. %v", id, err)
			} else {
				var v interface{}
				var gotarr []interface{}
				err = jsondoc.GetValue(godror.JSONOptNumberAsString, &v)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				}
				if gotarr, ok = v.([]interface{}); !ok {
					t.Errorf("%d. %T is not JSONArray ", id, v)
				}
				eq := reflect.DeepEqual(tC.JDOC, gotarr)
				t.Logf("%d. Got  Document read %+v): ", id, gotarr)
				if !eq {
					t.Errorf("Got %+v, wanted %+v", gotarr, tC.JDOC)
				}
			}
		}
		rows.Close()
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
		for rows.Next() {
			var id, person, personBirthDate interface{}
			var persontype, personDOBType string
			if err = rows.Scan(&id, &person, &personBirthDate, &persontype, &personDOBType); err != nil {
				rows.Close()
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

				var v interface{}
				var dobarr []interface{}
				err = personBirthDateJSON.GetValue(godror.JSONOptNumberAsString, &v)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				}
				if dobarr, ok = v.([]interface{}); !ok {
					t.Errorf("%d. %T is not JSONArray ", id, v)
				}
				for _, entry := range dobarr {
					if entry != birthdate {
						t.Errorf("Got %+v, wanted %+v", entry, birthdate)
					}
				}
			}
		}
		rows.Close()
	}
}

// It retrieves the object , person from JSON coloumn and updates
// BirthDate . We again read the BirthDate and verify its matching with
// what is written.
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
		jsonval, err := godror.NewJSONValue(tC.JDOC)
		if err != nil {
			t.Errorf("%d/3. %v", tN, err)
		}
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
			var id interface{}
			var personJSON godror.JSON
			if err = rows.Scan(&id, &personJSON); err != nil {
				t.Errorf("%d/3. scan: %v", tN, err)
				continue
			}

			var v interface{}
			var personMap map[string]interface{}
			err = personJSON.GetValue(godror.JSONOptNumberAsString, &v)
			if err != nil {
				t.Errorf("%d. %v", tN, err)
			}
			if personMap, ok = v.(map[string]interface{}); !ok {
				t.Errorf("%d. %T is not JSONObject ", id, v)
			}

			// Update BirthDate
			personMap["BirthDate"] = newBirthDate
			var newPersonObj godror.JSONObject
			// Get new JSONObject to be pushed to DB
			jsonval, err := godror.NewJSONValue(personMap)
			if err != nil {
				t.Errorf("%d. %v", id, err)
			}
			if newPersonObj, ok = jsonval.(godror.JSONObject); !ok {
				t.Errorf("%d Casting to JsonObject Failed", tN)
			}
			defer newPersonObj.Close()
			qry := "update " + tbl + " c set c.jdoc=JSON_TRANSFORM(c.jdoc, set '$.person'=:1)"
			_, err = conn.ExecContext(ctx, qry, jsonval)
			if err != nil {
				t.Errorf("%d. %v", id, err)
			}

			// Verify updated BirthDate by reading DB
			// tbd replace this with queryRowContext
			qry = "SELECT c.jdoc.person.BirthDate FROM " + tbl + " c where id =:1"
			row1, err := conn.QueryContext(ctx, qry, tN*2)
			if err != nil {
				t.Errorf("%d. %v", id, err)
			}
			var birthDateJSON godror.JSON
			for row1.Next() {
				if err = row1.Scan(&birthDateJSON); err != nil {
					t.Errorf("%d. %v", id, err)
				}

				var birthDateScalar interface{}
				var gotDOB time.Time
				err = birthDateJSON.GetValue(godror.JSONOptDefault, &birthDateScalar)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				}
				if gotDOB, ok = birthDateScalar.(time.Time); !ok {
					t.Errorf("%d. %T is not TimeStamp ", id, birthDateScalar)
				}
				if gotDOB != newBirthDate {
					t.Errorf("Got %+v, wanted %+v", gotDOB, newBirthDate)
				}
			}
			row1.Close()
		}
	}
}
