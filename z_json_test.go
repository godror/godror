// Copyright 2018, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// Batch Insert the JSON Formatted string without extended types.
// Reads from DB as standard JSON and compare with input JSON string.
// The Dates below are not stored as Oracle extended type, timestamp,
// instead they are stored as string.
// We need to use eJSON to retain types from JSON string.
// Alternatively if application uses map, array ,
// the extended types can be retained as show in tests, TestReadWriteJSONMap
// and TestReadWriteJSONArray.
//
// The float values are received as strings because DB native type NUMBER
// is converted to godror.Number.
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
		if errIs(err, 902, "invalid datatype") {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	t.Logf(" JSON Document table  %q: ", tbl)

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

	injs := "{\"person\":{\"Name\":\"Alex\",\"BirthDate\":\"1999-02-03T00:00:00\",\"ID\":12,\"JoinDate\":\"2020-11-24T12:34:56.123000Z\",\"age\":25,\"creditScore\":[700,250,340],\"salary\":45.23"

	// make the ints/floats as strings
	wantjs := "{\"person\":{\"Name\":\"Alex\",\"BirthDate\":\"1999-02-03T00:00:00\",\"ID\":\"12\",\"JoinDate\":\"2020-11-24T12:34:56.123000Z\",\"age\":\"25\",\"creditScore\":[\"700\",\"250\",\"340\"],\"salary\":\"45.23\""

	// Generate random string to get different JSON strings for batch insert
	const num = 100
	ids := make([]godror.Number, num)
	indocs := make([]godror.JSONString, num)
	wantdocs := make([]godror.JSONString, num)
	var sb strings.Builder
	var rs string // random string
	for i := range ids {
		// build input JSONString
		sb.WriteString(injs)
		sb.WriteString(",\"RandomString\":")
		sb.WriteString("\"")
		rs = getRandomString()
		sb.WriteString(rs)
		sb.WriteString("\"")
		sb.WriteString("}}")
		indocs[i] = godror.JSONString{Value: sb.String()}
		sb.Reset()

		// build expected JSONString from DB
		sb.WriteString(wantjs)
		sb.WriteString(",\"RandomString\":")
		sb.WriteString("\"")
		sb.WriteString(rs)
		sb.WriteString("\"")
		sb.WriteString("}}")
		wantdocs[i] = godror.JSONString{Value: sb.String()}
		ids[i] = godror.Number(strconv.Itoa(i))
		sb.Reset()
	}
	for tN, tC := range []struct {
		JDOC interface{}
		ID   interface{}
	}{
		{JDOC: indocs, ID: ids},
	} {
		if _, err = stmt.ExecContext(ctx, tC.ID, tC.JDOC); err != nil {
			t.Errorf("%d/1. (%v): %v", tN, tC.JDOC, err)
			continue
		}
		var rows *sql.Rows
		rows, err = conn.QueryContext(ctx,
			"SELECT id, jdoc FROM "+tbl+" ") //nolint:gas
		if err != nil {
			t.Errorf("%d/3. %v", tN, err)
			continue
		}
		var id int
		var jsondoc godror.JSON
		for rows.Next() {
			if err = rows.Scan(&id, &jsondoc); err != nil {
				rows.Close()
				t.Errorf("%d/3. scan: %v", tN, err)
				continue
			}
			t.Logf("%d. JSON Document read %q: ", id, jsondoc)
			got := jsondoc.String()
			if got == "" {
				t.Errorf("%d. %v", id, err)
			} else {
				eq, err := isEqualJSONString(got, wantdocs[id].Value)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				}
				if !eq {
					t.Errorf("%d. got %q for JDOC, wanted %q", id, got, wantdocs[id].Value)
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

var birthdate, _ = time.Parse(time.UnixDate, "Wed Feb 25 11:06:39 PST 1990")

// It simulates batch insert of JSON Column and single row insert.
// Go map[string]interface{} type is inserted for JSON Column and
// then read the JSON Document from DB.
// converts JSON Document into map[string]interface{}
// and compares with source.
//
// All int8, int16, int32, int64, float32, float64, godror.Number, uint8,
// uint16, uint32, uint64 are stored as NUMBER in DB.
//
// We are sending godror.Number types from map because with option
// JSONOptNumberAsString, we get DB NUMBER type as godor.Number.
// If we send JSONOptDefault, NUMBER type is converted to float64.
// use option, JSONOptDefault if the precision is with in float64 range
//
// Application can always convert to required types
// using conversions after fetching from DB.
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
		if errIs(err, 902, "invalid datatype") {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	t.Logf(" JSON Document table  %q: ", tbl)

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
	var travelTime time.Duration = 5*time.Hour + 21*time.Minute + 10*time.Millisecond + 20*time.Nanosecond
	jmap := map[string]interface{}{
		"person": map[string]interface{}{
			"ID":        godror.Number("12"),
			"FirstName": "Mary",
			"LastName":  "John",
			"creditScore": []interface{}{
				godror.Number("123456789123456789123456789123456789.12"),
				godror.Number("250"),
				godror.Number("340"),
			},
			"age":              godror.Number("25"),
			"BirthDate":        birthdate,
			"salary":           godror.Number("45.23"),
			"Local":            true,
			"BinData":          []byte{0, 1, 2, 3, 4},
			"TravelTimePerDay": travelTime,
		},
	}

	// values for batch insert
	const num = 40
	ids := make([]godror.Number, num)
	docs := make([]godror.JSONValue, num)
	for i := range ids {
		docs[i] = godror.JSONValue{Value: jmap}
		ids[i] = godror.Number(strconv.Itoa(i))
	}

	// value for last row to simulate single row insert
	lastIndex := godror.Number(strconv.Itoa(num))
	lastJSONDoc := godror.JSONValue{Value: jmap}

	for tN, tC := range []struct {
		ID   interface{}
		JDOC interface{}
	}{
		{JDOC: docs, ID: ids},
		{JDOC: lastJSONDoc, ID: lastIndex},
	} {
		if _, err = stmt.ExecContext(ctx, tC.ID, tC.JDOC); err != nil {
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
		var ok bool
		for rows.Next() {
			if err = rows.Scan(&id, &jsondoc); err != nil {
				rows.Close()
				t.Errorf("%d/3. scan: %v", tN, err)
				continue
			}

			if err != nil {
				t.Errorf("%d. %v", id, err)
			} else {
				t.Logf("%d. JSON Document read %q: ", id, jsondoc)

				var gotmap map[string]interface{}
				v, err := jsondoc.GetValue(godror.JSONOptNumberAsString)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				}
				if gotmap, ok = v.(map[string]interface{}); !ok {
					t.Errorf("%d. %T is not JSONObject ", id, v)
				}
				eq := reflect.DeepEqual(jmap, gotmap)
				if !eq {
					t.Errorf("Got %+v, wanted %+v", gotmap, jmap)
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
		if errIs(err, 902, "invalid datatype") {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	t.Logf(" JSON Document table  %q: ", tbl)

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
	jsarray := []interface{}{
		map[string]interface{}{
			"person": map[string]interface{}{
				"ID":        godror.Number("12"),
				"FirstName": "Mary",
				"LastName":  "John",
				"creditScore": []interface{}{
					godror.Number("700"),
					godror.Number("250"),
					godror.Number("340"),
				},
				"age":       godror.Number("25"),
				"BirthDate": birthdate,
				"salary":    godror.Number("4500.2351"),
				"Local":     true,
			},
		},
		map[string]interface{}{
			"person": map[string]interface{}{
				"ID":        godror.Number("13"),
				"FirstName": "Ivan",
				"LastName":  "John",
				"creditScore": []interface{}{
					godror.Number("800"),
					godror.Number("550"),
					godror.Number("340"),
				},
				"age":       godror.Number("22"),
				"BirthDate": birthdate,
				"salary":    godror.Number("4800.2351"),
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
		jsonval := godror.JSONValue{Value: tC.JDOC}
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
			if err != nil {
				t.Errorf("%d. %v", id, err)
			} else {
				t.Logf("%d. JSON Document read %q: ", id, jsondoc)
				var gotarr []interface{}
				var ok bool
				v, err := jsondoc.GetValue(godror.JSONOptNumberAsString)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				}
				if gotarr, ok = v.([]interface{}); !ok {
					t.Errorf("%d. %T is not JSONArray ", id, v)
				}
				eq := reflect.DeepEqual(tC.JDOC, gotarr)
				if !eq {
					t.Errorf("Got %+v, wanted %+v", gotarr, tC.JDOC)
				}
			}
		}
		rows.Close()
	}
}

// It fetches field, person from the JSON coloumn which is
// returned as array as there are multiple person objects.
// It then fetches field, birthdates of each person and is
// validated with what is inserted.
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
		if errIs(err, 902, "invalid datatype") {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	t.Logf(" JSON Document table  %q: ", tbl)

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
	jsarray := []interface{}{
		map[string]interface{}{
			"person": map[string]interface{}{
				"ID":        godror.Number("12"),
				"FirstName": "Mary",
				"LastName":  "John",
				"creditScore": []interface{}{
					godror.Number("700"),
					godror.Number("250"),
					godror.Number("340"),
				},
				"age":       godror.Number("25"),
				"BirthDate": birthdate,
				"salary":    godror.Number("4500.2351"),
				"Local":     true,
			},
		},
		map[string]interface{}{
			"person": map[string]interface{}{
				"ID":        godror.Number("13"),
				"FirstName": "Ivan",
				"LastName":  "John",
				"creditScore": []interface{}{
					godror.Number("800"),
					godror.Number("550"),
					godror.Number("340"),
				},
				"age":       godror.Number("22"),
				"BirthDate": birthdate,
				"salary":    godror.Number("4800.2351"),
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
		jsonval := godror.JSONValue{Value: tC.JDOC}
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
			var id int
			var personJSON, personBirthDateJSON godror.JSON
			var persontype, personDOBType string
			if err = rows.Scan(&id, &personJSON, &personBirthDateJSON, &persontype, &personDOBType); err != nil {
				rows.Close()
				t.Errorf("%d/3. scan: %v", tN, err)
				continue
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
			// Display all person objects from JSON Column
			t.Logf("%d. JSON Document for Person %q: ", id, personJSON)

			// Get all Birthdates and verify
			var dobarr []interface{}
			var ok bool
			v, err := personBirthDateJSON.GetValue(godror.JSONOptNumberAsString)
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
		rows.Close()
	}
}

// It retrieves the object, person from JSON coloumn and updates
// BirthDate. It then binds the map as a JSON scalar.
// object person LastName is changed by binding string as a JSON scalar.
//
// We again read the BirthDate, LastName from DB and verify its matching with
// what is written.
func TestUpdateJSONScalar(t *testing.T) {
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
		if errIs(err, 902, "invalid datatype") {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	t.Logf(" JSON Document table  %q: ", tbl)

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
	newBirthDate, _ := time.Parse(time.UnixDate, "Wed Feb 25 11:06:39 PST 1989")
	jsarray := []interface{}{
		map[string]interface{}{
			"person": map[string]interface{}{
				"ID":        godror.Number("12"),
				"FirstName": "Mary",
				"LastName":  "John",
				"creditScore": []interface{}{
					godror.Number("700"),
					godror.Number("250"),
					godror.Number("340"),
				},
				"age":       godror.Number("25"),
				"BirthDate": birthdate,
				"salary":    godror.Number("4500.2351"),
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
		jsonval := godror.JSONValue{Value: tC.JDOC}
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
			var personMap map[string]interface{}
			var ok bool
			v, err := personJSON.GetValue(godror.JSONOptNumberAsString)
			if err != nil {
				t.Errorf("%d. %v", tN, err)
			}
			if personMap, ok = v.(map[string]interface{}); !ok {
				t.Errorf("%d. %T is not JSONObject ", id, v)
			}

			// Update BirthDate
			personMap["BirthDate"] = newBirthDate
			// Get new JSONObject to be pushed to DB
			jsonval := godror.JSONValue{personMap}
			qry := "update " + tbl + " c set c.jdoc=JSON_TRANSFORM(c.jdoc, set '$.person'=:1)"
			// binding map object
			_, err = conn.ExecContext(ctx, qry, jsonval)
			if err != nil {
				t.Errorf("%d. %v", id, err)
			}

			// Update LastName directly as string.
			wantLastName := "Ivan"
			qry = "update " + tbl + " c set c.jdoc=JSON_TRANSFORM(c.jdoc, set '$.person.LastName'=:1 )"
			// binding string
			_, err = conn.ExecContext(ctx, qry, wantLastName)
			if err != nil {
				t.Errorf("%d. %v", id, err)
			}

			// Verify updated BirthDate and LastName by reading DB
			// tbd replace this with queryRowContext
			qry = "SELECT c.jdoc.person.BirthDate, c.jdoc.person.LastName  FROM " + tbl + " c where id =:1"
			row1, err := conn.QueryContext(ctx, qry, tN*2)
			if err != nil {
				t.Errorf("%d. %v", id, err)
			}
			var birthDateJSON godror.JSON
			var lastNameJSON godror.JSON
			var gotDOB time.Time
			var gotLastName string
			for row1.Next() {
				if err = row1.Scan(&birthDateJSON, &lastNameJSON); err != nil {
					t.Errorf("%d. %v", id, err)
				}
				// Verify BirthDate
				birthDateScalar, err := birthDateJSON.GetValue(godror.JSONOptDefault)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				}
				if gotDOB, ok = birthDateScalar.(time.Time); !ok {
					t.Errorf("%d. %T is not TimeStamp ", id, birthDateScalar)
				}
				if gotDOB != newBirthDate {
					t.Errorf("Got %+v, wanted %+v", gotDOB, newBirthDate)
				}

				// Verify LastName
				lastNameScalar, err := lastNameJSON.GetValue(godror.JSONOptDefault)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				}
				if gotLastName, ok = lastNameScalar.(string); !ok {
					t.Errorf("%d. %T is not String ", id, lastNameScalar)
				}
				if gotLastName != wantLastName {
					t.Errorf("Got %+v, wanted %+v", gotLastName, wantLastName)
				}
			}
			row1.Close()
		}
	}
}

func errIs(err error, code int, msg string) bool {
	if err == nil {
		return false
	}
	var ec interface{ Code() int }
	if errors.As(err, &ec) {
		return ec.Code() == code
	}
	return msg != "" && strings.Contains(err.Error(), msg)
}
