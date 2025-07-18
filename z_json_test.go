// Copyright 2018, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
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

	"github.com/google/go-cmp/cmp"
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
	for i := range ids {
		// build input JSONString
		sb.WriteString(injs)
		sb.WriteString(",\"RandomString\":")
		sb.WriteString("\"")
		rs := getRandomString()
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
		if _, err = stmt.ExecContext(ctx,
			tC.ID, tC.JDOC,
		); err != nil {
			t.Errorf("%d/1. (%v): %v", tN, tC.JDOC, err)
			continue
		}
		var rows *sql.Rows
		if rows, err = conn.QueryContext(ctx,
			"SELECT id, jdoc FROM "+tbl, //nolint:gas
			godror.JSONStringOption(godror.JSONOptNumberAsString),
		); err != nil {
			t.Errorf("%d/3. %v", tN, err)
			continue
		}
		defer rows.Close()
		var id int
		var jsondoc godror.JSON
		for rows.Next() {
			if err = rows.Scan(&id, &jsondoc); err != nil {
				rows.Close()
				t.Errorf("%d/3. scan: %v", tN, err)
				continue
			}
			t.Logf("%d. JSON Document %#[2]v read %[2]q: ", id, jsondoc)
			got := jsondoc.String()
			if got == "" {
				t.Errorf("%d. %v", id, err)
			} else {
				d, err := diffJSONString(got, wantdocs[id].Value)
				if err != nil {
					t.Errorf("%d. %v", id, err)
				}
				if d != "" {
					t.Errorf("%d. got %q for JDOC, wanted %q:\n%s", id, got, wantdocs[id].Value, d)
					break
				}
			}
		}
		rows.Close()
	}
}

// Check if two JSON strings are equal ignoring the order
func diffJSONString(js1, js2 string) (string, error) {
	var js1type interface{}
	var js2type interface{}

	var err error
	err = json.Unmarshal([]byte(js1), &js1type)
	if err != nil {
		return "", err
	}
	err = json.Unmarshal([]byte(js2), &js2type)
	if err != nil {
		return "", err
	}
	return cmp.Diff(js1type, js2type), nil

}

var birthdate = time.Date(1990, 2, 25, 11, 6, 39, 0, time.Local)

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
				if d := cmp.Diff(jmap, gotmap); d != "" {
					t.Fatalf("got %+v, wanted %+v:\n%s", gotmap, jmap, d)
				}
			}

		}
		rows.Close()
	}
}

// It inserts Go Array []interface{} and reads the JSON Document from DB.
// converts JSON Document into []interface{} and compares with source
func TestReadWriteJSONArray(t *testing.T) {
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
				if d := cmp.Diff(tC.JDOC, gotarr); d != "" {
					t.Errorf("Got %+v, wanted %+v:\n%s", gotarr, tC.JDOC, d)
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
	newBirthDate := birthdate.AddDate(-1, 0, 0)
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

			if err = func() error {
				// Verify updated BirthDate and LastName by reading DB
				// tbd replace this with queryRowContext
				qry := "SELECT c.jdoc.person.BirthDate, c.jdoc.person.LastName  FROM " + tbl + " c where id =:1"
				row1, err := conn.QueryContext(ctx, qry, tN*2)
				if err != nil {
					return fmt.Errorf("%d. %v", id, err)
				}
				defer row1.Close()
				var birthDateJSON godror.JSON
				var lastNameJSON godror.JSON
				var gotDOB time.Time
				var gotLastName string
				for row1.Next() {
					if err = row1.Scan(&birthDateJSON, &lastNameJSON); err != nil {
						return fmt.Errorf("%d. %v", id, err)
					}
					// Verify BirthDate
					birthDateScalar, err := birthDateJSON.GetValue(godror.JSONOptDefault)
					if err != nil {
						return fmt.Errorf("%d. %v", id, err)
					}
					if gotDOB, ok = birthDateScalar.(time.Time); !ok {
						return fmt.Errorf("%d. %T is not TimeStamp ", id, birthDateScalar)
					}
					if gotDOB != newBirthDate {
						return fmt.Errorf("Got %+v, wanted %+v", gotDOB, newBirthDate)
					}

					// Verify LastName
					lastNameScalar, err := lastNameJSON.GetValue(godror.JSONOptDefault)
					if err != nil {
						return fmt.Errorf("%d. %v", id, err)
					}
					if gotLastName, ok = lastNameScalar.(string); !ok {
						return fmt.Errorf("%d. %T is not String ", id, lastNameScalar)
					}
					if gotLastName != wantLastName {
						return fmt.Errorf("Got %+v, wanted %+v", gotLastName, wantLastName)
					}
				}
				return row1.Close()
			}(); err != nil {
				t.Error(err)
			}
		}
	}
}

// Converts map with different Go-types to godror.JSON type
// This is bound to the insert function and executed
// For each unique go-type in the map, their corresponding JSON types,
// as stored in the DB, are fetched and compared with their expected values.
func TestJSONStorageTypes(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("StorageTypes"),
		30*time.Second)
	defer cancel()

	tbl := "test_objectcollection_jsonmap" + tblSuffix
	testData := []struct {
		Key   string
		Value interface{}
	}{
		{"asNumber", godror.Number("12")},
		{"asString", "Mary"},
		{"asTimestamp", birthdate},
		{"asBoolean", true},
		{"asByte", []byte{45, 51}},
		{"asInt32", int32(98)},
		{"asInt64", int64(99)},
		{"asInt8", int8(10)},
		{"asInt16", int16(20)},
		{"asUint64", uint64(99)},
		{"asFloat64", float64(98.11)},
		{"asFloat32", float32(97.2)},
	}
	// Map with different Go-types, which would be stored as JSON in the DB
	obj := make(map[string]interface{}, len(testData))
	var buf strings.Builder
	buf.WriteString("SELECT id, json_value(jdoc,'$.asObject.type()')")
	for _, elt := range testData {
		obj[elt.Key] = elt.Value
		fmt.Fprintf(&buf, ",\n\tjson_value(jdoc,'$.asObject.%s.type()')", elt.Key)
	}
	buf.WriteString("\nFROM " + tbl + " c ")
	qry := buf.String()
	jsmap := map[string]interface{}{"asObject": obj}
	t.Log("qry:", qry)

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
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

	jsonval := godror.JSONValue{Value: jsmap}
	if _, err = stmt.ExecContext(ctx, 0, jsonval); err != nil {
		t.Errorf("%d/1. (%v): %v", 0, jsmap, err)
	}

	rows, err := conn.QueryContext(ctx, qry)
	if err != nil {
		t.Errorf("%d/3. %s: %+v", 0, qry, err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var asStringtype, asTimestamptype, asObjecttype, asBooleantype, asNumbertype, asInt32type, asInt64type,
			asInt8type, asInt16type, asUint64type,
			asFloat64type, asFloat32type, asBytetype string

		err = rows.Scan(&id, &asObjecttype,
			&asNumbertype, &asStringtype, &asTimestamptype,
			&asBooleantype, &asBytetype, &asInt32type,
			&asInt64type, &asInt8type, &asInt16type,
			&asUint64type, &asFloat64type, &asFloat32type)
		if err != nil {
			t.Errorf("%d/3. scan: %v", 0, err)
			break
		}

		// Valid DB types
		wantObjectDBtype := "object"
		wantNumberDBtype := "number"
		wantStringDBtype := "string"
		wantTimestampDBtype := "timestamp"
		wantBooleanDBtype := "boolean"
		wantBinaryDBtype := "binary"

		for _, tCase := range []struct {
			goType            string
			getType, wantType string
		}{
			{"map", asObjecttype, wantObjectDBtype},
			{"string", asStringtype, wantStringDBtype},
			{"godror.number", asNumbertype, wantNumberDBtype},
			{"boolean", asBooleantype, wantBooleanDBtype},
			{"time.Time", asTimestamptype, wantTimestampDBtype},
			{"int32", asInt32type, wantNumberDBtype},
			{"int64", asInt64type, wantNumberDBtype},
			{"int8", asInt8type, wantNumberDBtype},
			{"int16", asInt16type, wantNumberDBtype},
			{"uint64", asUint64type, wantNumberDBtype},
			{"float64", asFloat64type, wantNumberDBtype},
			{"float32", asFloat32type, wantNumberDBtype},
			{"[]byte", asBytetype, wantBinaryDBtype},
		} {
			if tCase.getType != tCase.wantType {
				t.Errorf("For go-type %+v, got %+v, wanted %+v",
					tCase.goType, tCase.getType, tCase.wantType)
			}
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

func TestJSONIssue371(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("StorageTypes"),
		30*time.Second)
	defer cancel()
	const jsonstring = "{\"person\":{\"BirthDate\":\"1999-02-03T00:00:00\",\"ID\":\"12\",\"JoinDate\":\"2020-11-24T12:34:56.123000Z\",\"Name\":\"Alex\",\"RandomString\":\"APKZYKSv2\",\"age\":\"25\",\"creditScore\":[\"700\",\"250\",\"340\"],\"salary\":\"45.23\"}}"
	jsonval := godror.JSONString{Value: jsonstring, Flags: 0}
	tbl := "test_issue371" + tblSuffix

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	if err != nil {
		if errIs(err, 902, "invalid datatype") {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), "DROP TABLE "+tbl)

	if _, err = conn.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (id NUMBER(6), jdoc JSON)", //nolint:gas
	); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.ExecContext(ctx, "INSERT INTO "+tbl+"(id, jdoc) VALUES(1 , :1)", jsonval); err != nil {
		t.Fatal(err)
	}

	// Read JSON document
	rows, err := conn.QueryContext(ctx, "SELECT id, jdoc FROM "+tbl)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var id interface{}
	var jsondoc godror.JSON

	for rows.Next() {
		if err = rows.Scan(&id, &jsondoc); err != nil {
			t.Fatal(err)
		}
		// Print JSON string
		t.Log("The JSON String is:", jsondoc)
		// Get Go native map[string]interface{}
		v, _ := jsondoc.GetValue(godror.JSONOptNumberAsString)
		// type assert to verify the type returned
		gotmap, _ := v.(map[string]interface{})
		t.Log("The JSON Map object is:", gotmap)
	}
}

// jsonDataType mimics what GORM.JSON does
type jsonDataType json.RawMessage

func (j jsonDataType) Value() (driver.Value, error) {
	if len(j) == 0 {
		return nil, nil
	}
	return string(j), nil
}

func (j *jsonDataType) Scan(value interface{}) error {
	if value == nil {
		*j = jsonDataType("null")
		return nil
	}
	var bytes []byte
	if s, ok := value.(fmt.Stringer); ok {
		bytes = []byte(s.String())
	} else {
		switch v := value.(type) {
		case []byte:
			if len(v) > 0 {
				bytes = make([]byte, len(v))
				copy(bytes, v)
			}
		case string:
			bytes = []byte(v)
		default:
			return fmt.Errorf("Failed to unmarshal JSONB value: %+v", value)
		}
	}

	result := json.RawMessage(bytes)
	*j = jsonDataType(result)
	return nil
}

// TestReadWriteJSONRawMessage - Inserts json.RawMessage datatype and reads the JSON Document from DB.
// Example Setup:
// CREATE USER IF NOT EXISTS demo IDENTIFIED BY demo;
// ALTER USER demo quota unlimited ON system;
// GRANT ALL PRIVILEGES TO demo;
// CREATE TABLESPACE demo_ts DATAFILE 'demo.dat' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE 500M online;
// ALTER USER demo DEFAULT TABLESPACE demo_ts;
// ALTER USER demo quota unlimited on demo_ts;

// Example cleanup:
// DROP USER demo;
// ALTER DATABASE DATAFILE 'demo.dat' OFFLINE DROP;
// DROP TABLESPACE demo_ts INCLUDING CONTENTS and DATAFILES;
func TestReadWriteJSONRawMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("ReadWriteJsonRawMessage"), 30*time.Second)
	defer cancel()

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tbl := "test_personcollection_jsonraw" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	_, err = conn.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (id NUMBER(6), jdoc JSON, CONSTRAINT ID_PK PRIMARY KEY (id))", //nolint:gas
	)
	if err != nil {
		t.Error(err)
	}
	t.Logf(" JSON Document table  %q: ", tbl)

	defer testDb.Exec(
		"DROP TABLE " + tbl, //nolint:gas
	)
	stmt, err := conn.PrepareContext(ctx,
		"INSERT INTO "+tbl+" (id, jdoc) VALUES (:1, :2)", //nolint:gas
	)
	if err != nil {
		t.Error(err)
	}
	defer stmt.Close()

	testData := `{"string":"Hello World"}`
	rawMessage := jsonDataType(testData)

	if _, err = stmt.ExecContext(ctx, 1, rawMessage); err != nil {
		t.Errorf("%d/1. (%v): %v", 1, rawMessage, err)
		return
	}

	rows, err := conn.QueryContext(ctx,
		"SELECT * FROM "+tbl+" c ",
		godror.JSONAsString(),
	) //nolint:gas
	if err != nil {
		t.Errorf("%d/3. %v", 1, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Errorf("Failed to get columns: %v", err)
	}
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range cols {
		valuePtrs[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		for i, col := range cols {
			// fmt.Printf("%s: %v\t", col, values[i])
			switch i {
			case 0: // ID
				if values[0] != int64(1) {
					t.Errorf("Column %s: got %[2]v with type: %[2]T, wanted: %[3]v with type: %[3]T", col, values[0], int64(1))
				}
			case 1: // jdoc
				if values[1] != testData {
					t.Errorf("Column %s: got %[2]v with type: %[2]T, wanted %[3]v with type: %[3]T", col, values[1], testData)
				}
			default:
				t.Errorf("Unsupported column index:%d", i)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Errorf("Row iteration error: %v", err)
	}
}
