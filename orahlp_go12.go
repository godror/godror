// +build !go1.13

// Copyright 2017, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"context"
	"database/sql"
)

// Raw executes f on the given *sql.DB or *sql.Conn.
func Raw(ctx context.Context, ex Execer, f func(driverConn Conn) error) error {
	var err error
	if conner, ok := ex.(interface {
		Conn(context.Context) (*sql.Conn, error)
	}); ok {
		conn, cErr := conner.Conn(ctx)
		if cErr != nil {
			return cErr
		}
		defer conn.Close()
		ex = conn
	} else if txer, ok := ex.(interface {
		BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	}); ok {
		tx, txErr := txer.BeginTx(ctx, nil)
		if txErr != nil {
			return txErr
		}
		defer func() {
			if err != nil {
				tx.Rollback()
			} else {
				err = tx.Commit()
			}
		}()
		ex = tx
	}

	var cx *conn
	if cx, err = getConn(ctx, ex); err != nil {
		return err
	}
	defer cx.Close()
	return f(cx)
}
