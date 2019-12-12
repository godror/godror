// Copyright 2018 Kurt K, Tamás Gulácsi.
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package goracle_test

import (
	"context"
	"database/sql"
	"log"

	errors "golang.org/x/xerrors"

	goracle "gopkg.in/goracle.v2"
)

// ExampleStartup calls exampleStartup to start a database.
func ExampleStartup() {
	if err := exampleStartup(goracle.StartupDefault); err != nil {
		log.Fatal(err)
	}
}
func exampleStartup(startupMode goracle.StartupMode) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dsn := "oracle://?sysdba=1&prelim=1"
	db, err := sql.Open("goracle", dsn)
	if err != nil {
		log.Fatal(errors.Errorf("%s: %w", dsn, err))
	}
	defer db.Close()

	oraDB, err := goracle.DriverConn(ctx, db)
	if err != nil {
		return err
	}
	log.Println("Starting database")
	if err = oraDB.Startup(startupMode); err != nil {
		return err
	}
	// You cannot alter database on the prelim_auth connection.
	// So open a new connection and complete startup, as Startup starts pmon.
	db2, err := sql.Open("goracle", "oracle://?sysdba=1")
	if err != nil {
		return err
	}
	defer db2.Close()

	log.Println("Mounting database")
	if _, err = db2.Exec("alter database mount"); err != nil {
		return err
	}
	log.Println("Opening database")
	if _, err = db2.Exec("alter database open"); err != nil {
		return err
	}
	return nil
}

// ExampleShutdown is an example of how to shut down a database.
func ExampleShutdown() {
	dsn := "oracle://?sysdba=1" // equivalent to "/ as sysdba"
	db, err := sql.Open("goracle", dsn)
	if err != nil {
		log.Fatal(errors.Errorf("%s: %w", dsn, err))
	}
	defer db.Close()

	if err = exampleShutdown(db, goracle.ShutdownTransactionalLocal); err != nil {
		log.Fatal(err)
	}
}

func exampleShutdown(db *sql.DB, shutdownMode goracle.ShutdownMode) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oraDB, err := goracle.DriverConn(ctx, db)
	if err != nil {
		return err
	}
	log.Printf("Beginning shutdown %v", shutdownMode)
	if err = oraDB.Shutdown(shutdownMode); err != nil {
		return err
	}
	// If we abort the shutdown process is over immediately.
	if shutdownMode == goracle.ShutdownAbort {
		return nil
	}

	log.Println("Closing database")
	if _, err = db.Exec("alter database close normal"); err != nil {
		return err
	}
	log.Println("Unmounting database")
	if _, err = db.Exec("alter database dismount"); err != nil {
		return err
	}
	log.Println("Finishing shutdown")
	if err = oraDB.Shutdown(goracle.ShutdownFinal); err != nil {
		return err
	}
	return nil
}
