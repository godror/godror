// Copyright 2018, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	"database/sql"
	"log"

	errors "golang.org/x/xerrors"

	godror "github.com/godror/godror"
)

// ExampleStartupMode calls exampleStartup to start a database.
func ExampleStartupMode() {
	if err := exampleStartup(godror.StartupDefault); err != nil {
		log.Fatal(err)
	}
}
func exampleStartup(startupMode godror.StartupMode) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dsn := "oracle://?sysdba=1&prelim=1"
	db, err := sql.Open("godror", dsn)
	if err != nil {
		log.Fatal(errors.Errorf("%s: %w", dsn, err))
	}
	defer db.Close()

	oraDB, err := godror.DriverConn(ctx, db)
	if err != nil {
		return err
	}
	log.Println("Starting database")
	if err = oraDB.Startup(startupMode); err != nil {
		return err
	}
	// You cannot alter database on the prelim_auth connection.
	// So open a new connection and complete startup, as Startup starts pmon.
	db2, err := sql.Open("godror", "oracle://?sysdba=1")
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

// ExampleShutdownMode is an example of how to shut down a database.
func ExampleShutdownMode() {
	dsn := "oracle://?sysdba=1" // equivalent to "/ as sysdba"
	db, err := sql.Open("godror", dsn)
	if err != nil {
		log.Fatal(errors.Errorf("%s: %w", dsn, err))
	}
	defer db.Close()

	if err = exampleShutdown(db, godror.ShutdownTransactionalLocal); err != nil {
		log.Fatal(err)
	}
}

func exampleShutdown(db *sql.DB, shutdownMode godror.ShutdownMode) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oraDB, err := godror.DriverConn(ctx, db)
	if err != nil {
		return err
	}
	log.Printf("Beginning shutdown %v", shutdownMode)
	if err = oraDB.Shutdown(shutdownMode); err != nil {
		return err
	}
	// If we abort the shutdown process is over immediately.
	if shutdownMode == godror.ShutdownAbort {
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
	if err = oraDB.Shutdown(godror.ShutdownFinal); err != nil {
		return err
	}
	return nil
}
