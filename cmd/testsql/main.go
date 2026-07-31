// Copyright 2026 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

// Package main of testsql is a program that allows testing DML queries against a DB.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/peterbourgon/ff/v4"
	"github.com/peterbourgon/ff/v4/ffhelp"

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
			db, err := sql.Open("godror", *flagConnect)
			if err != nil {
				return fmt.Errorf("connect to %q: %w", *flagConnect, err)
			}
			defer db.Close()
			tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				return err
			}
			defer tx.Rollback()
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
			stmt, err := tx.PrepareContext(ctx, qry)
			if err != nil {
				return fmt.Errorf("prepare %s: %w", qry, err)
			}
			stmt.Close()
			return nil
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
