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

package goracle

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

// QueryColumn is the described column.
type QueryColumn struct {
	Schema, Name                   string
	Type, Length, Precision, Scale int
	Nullable                       bool
	CharsetID, CharsetForm         int
}

type execer interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

// DescribeQuery describes the columns in the qry string,
// using DBMS_SQL.PARSE + DBMS_SQL.DESCRIBE_COLUMNS2.
//
// This can help using unknown-at-compile-time, a.k.a.
// dynamic queries.
func DescribeQuery(ctx context.Context, db execer, qry string) ([]QueryColumn, error) {
	//res := strings.Repeat("\x00", 32767)
	res := make([]byte, 32767)
	if _, err := db.ExecContext(ctx, `DECLARE
  c INTEGER;
  col_cnt INTEGER;
  rec_tab DBMS_SQL.DESC_TAB;
  a DBMS_SQL.DESC_REC;
  v_idx PLS_INTEGER;
  res VARCHAR2(32767);
BEGIN
  c := DBMS_SQL.OPEN_CURSOR;
  BEGIN
    DBMS_SQL.PARSE(c, :1, DBMS_SQL.NATIVE);
    DBMS_SQL.DESCRIBE_COLUMNS(c, col_cnt, rec_tab);
    v_idx := rec_tab.FIRST;
    WHILE v_idx IS NOT NULL LOOP
      a := rec_tab(v_idx);
      res := res||a.col_schema_name||' '||a.col_name||' '||a.col_type||' '||
                  a.col_max_len||' '||a.col_precision||' '||a.col_scale||' '||
                  (CASE WHEN a.col_null_ok THEN 1 ELSE 0 END)||' '||
                  a.col_charsetid||' '||a.col_charsetform||
                  CHR(10);
      v_idx := rec_tab.NEXT(v_idx);
    END LOOP;
	--Loop ended, close cursor
    DBMS_SQL.CLOSE_CURSOR(c);
  EXCEPTION WHEN OTHERS THEN NULL;
    --Error happened, close cursor anyway!
    DBMS_SQL.CLOSE_CURSOR(c);
	RAISE;
  END;
  :2 := UTL_RAW.CAST_TO_RAW(res);
END;`, qry, &res,
	); err != nil {
		return nil, err
	}
	if i := bytes.IndexByte(res, 0); i >= 0 {
		res = res[:i]
	}
	lines := bytes.Split(res, []byte{'\n'})
	cols := make([]QueryColumn, 0, len(lines))
	var nullable int
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		var col QueryColumn
		switch j := bytes.IndexByte(line, ' '); j {
		case -1:
			continue
		case 0:
			line = line[1:]
		default:
			col.Schema, line = string(line[:j]), line[j+1:]
		}
		if n, err := fmt.Sscanf(string(line), "%s %d %d %d %d %d %d %d",
			&col.Name, &col.Type, &col.Length, &col.Precision, &col.Scale, &nullable, &col.CharsetID, &col.CharsetForm,
		); err != nil {
			return cols, errors.Wrapf(err, "parsing %q (parsed: %d)", line, n)
		}
		col.Nullable = nullable != 0
		cols = append(cols, col)
	}
	return cols, nil
}

// ServerVersion data.
type ServerVersion struct {
	// major.maintenance.application-server.component-specific.platform-specific
	Major, Maintenance, AppServer, Component, Platform int8
}

type queryRower interface {
	QueryRow(string, ...interface{}) *sql.Row
}

// GetServerVersion returns the Oracle product version.
func GetServerVersion(db queryRower) (ServerVersion, error) {
	var s sql.NullString
	if err := db.QueryRow("SELECT MIN(VERSION) FROM product_component_version " +
		" WHERE product LIKE 'Oracle Database%'").Scan(&s); err != nil {
		return ServerVersion{Major: -1}, err
	}
	var v ServerVersion
	if _, err := fmt.Sscanf(s.String, "%d.%d.%d.%d.%d",
		&v.Major, &v.Maintenance, &v.AppServer, &v.Component, &v.Platform); err != nil {
		return v, errors.Wrapf(err, "scan version number %q", s.String)
	}
	return v, nil
}

// CompileError represents a compile-time error as in user_errors view.
type CompileError struct {
	Owner, Name, Type    string
	Line, Position, Code int64
	Text                 string
	Warning              bool
}

func (ce CompileError) Error() string {
	prefix := "ERROR "
	if ce.Warning {
		prefix = "WARN  "
	}
	return fmt.Sprintf("%s %s.%s %s %d:%d [%d] %s",
		prefix, ce.Owner, ce.Name, ce.Type, ce.Line, ce.Position, ce.Code, ce.Text)
}

type queryer interface {
	Query(string, ...interface{}) (*sql.Rows, error)
}

// GetCompileErrors returns the slice of the errors in user_errors.
//
// If all is false, only errors are returned; otherwise, warnings, too.
func GetCompileErrors(queryer queryer, all bool) ([]CompileError, error) {
	rows, err := queryer.Query(`
	SELECT USER owner, name, type, line, position, message_number, text, attribute
		FROM user_errors
		ORDER BY name, sequence`)
	if err != nil {
		return nil, err
	}
	var errors []CompileError
	var warn string
	for rows.Next() {
		var ce CompileError
		if err = rows.Scan(&ce.Owner, &ce.Name, &ce.Type, &ce.Line, &ce.Position, &ce.Code, &ce.Text, &warn); err != nil {
			return errors, err
		}
		ce.Warning = warn == "WARNING"
		if !ce.Warning || all {
			errors = append(errors, ce)
		}
	}
	return errors, rows.Err()
}

type preparer interface {
	Prepare(string) (*sql.Stmt, error)
}

func EnableDbmsOutput(ctx context.Context, conn execer) error {
	qry := "BEGIN DBMS_OUTPUT.enable(1000000); END;"
	_, err := conn.ExecContext(ctx, qry)
	return errors.Wrap(err, qry)
}

func ReadDbmsOutput(ctx context.Context, w io.Writer, conn preparer) error {
	qry := `BEGIN DBMS_OUTPUT.get_lines(:1, :2); END;`
	stmt, err := conn.Prepare(qry)
	if err != nil {
		return errors.Wrap(err, qry)
	}

	lines := make([]string, 128)
	var numLines int64
	params := []interface{}{PlSQLArrays,
		sql.Out{Dest: &lines}, sql.Out{Dest: &numLines, In: true},
	}
	for {
		numLines = int64(len(lines))
		if _, err := stmt.ExecContext(ctx, params...); err != nil {
			return errors.Wrap(err, qry)
		}
		for i := 0; i < int(numLines); i++ {
			if _, err := io.WriteString(w, lines[i]); err != nil {
				return err
			}
		}
		if int(numLines) < len(lines) {
			return nil
		}
	}
	return nil
}
