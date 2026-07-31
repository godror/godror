// Copyright 2026 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

// Package main of testsql is a program that allows testing DML queries against a DB.
package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/peterbourgon/ff/v4"
	"github.com/peterbourgon/ff/v4/ffhelp"

	"github.com/godror/godror"
	_ "github.com/godror/godror"
)

func main() {
	if err := Main(); err != nil {
		log.Fatal(err)
	}
}

func Main() error {
	flags := ff.NewFlagSet("testsql")
	flagConnect := flags.String('c', "connect", "", "DSN to connect to")
	app := ff.Command{Name: "testsql", Flags: flags,
		Exec: func(ctx context.Context, args []string) error {
			var qry string
			if len(args) == 0 || (len(args) == 1 && args[0] == "" || args[0] == "-") {
				b, err := io.ReadAll(os.Stdin)
				if err != nil {
					return err
				}
				qry = string(b)
			} else {
				qry = strings.Join(args, " ")
			}
			log.Println("parse", qry)
			db, err := sql.Open("godror", *flagConnect)
			if err != nil {
				return fmt.Errorf("connect to %q: %w", *flagConnect, err)
			}
			defer db.Close()
			return godror.Raw(ctx, db, func(conn godror.Conn) error {
				tx, err := conn.BeginTx(ctx, driver.TxOptions{ReadOnly: true})
				if err != nil {
					return err
				}
				defer tx.Rollback()
				stmt, err := conn.PrepareContext(ctx, qry)
				if err != nil {
					return fmt.Errorf("prepare %s: %w", qry, err)
				}
				defer stmt.Close()
				cnt, names, err := stmt.(interface{ BindNames() (int, []string, error) }).BindNames()
				if err != nil {
					return err
				}
				args := make([]driver.Value, 0, min(cnt, len(names)))
				if cnt != 0 {
					if len(names) == 0 {
						for range cnt {
							args = append(args, "")
						}
					} else {
						for _, nm := range names {
							args = append(args, driver.NamedValue{Name: nm, Value: ""})
						}
					}
				}
				info := stmt.(interface{ Info() godror.StatementInfo }).Info()
				if info.IsDDL {
					return ErrDDL
				}
				var rows driver.Rows
				if !info.IsQuery {
					_, err = stmt.Exec(args)
				} else {
					rows, err = stmt.Query(args)
					if rows != nil {
						rows.Close()
					}
				}
				return err
			})
		},
	}
	if err := app.Parse(os.Args[1:]); err != nil {
		ffhelp.Command(&app).WriteTo(os.Stderr)
		if errors.Is(err, ff.ErrHelp) {
			return nil
		}
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	return app.Run(ctx)
}

var ErrDDL = errors.New("statement is a DDL")
