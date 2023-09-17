package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/godror/godror"
	// "github.com/jmoiron/sqlx"
)

var (
	connection = flag.String("connection", os.Getenv("GODROR_TEST_DSN"), "connection string")
	memProfFn  = flag.String("memprofile", "godror-benchmem.pprof", "memory profile file name")
	timeout    = flag.Duration("timeout", 5*time.Minute, "test timeout")
)

type Config struct {
	Connection string

	db *sql.DB
}

type Exporter struct {
	config *Config
}

func NewExporter(config *Config) *Exporter {
	return &Exporter{
		config: config,
	}
}

func (e *Exporter) Query(ctx context.Context, query string, function func(rows *sql.Rows) bool) bool {
	config := e.config
	db := config.db

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		slog.Error(config.Connection, query, err)
		return false
	}

	defer func() {
		if rows != nil {
			err := rows.Close()

			if err != nil {
				slog.Error("rows", "connecion", config.Connection, "qry", query, "error", err)
			} else {
				slog.Debug("rows closed", "connection", config.Connection, "query", query)
			}
		}
	}()

	for rows.Next() {
		if !function(rows) {
			break
		}
	}

	return true
}

// Connect the DB and gather Databasename and Instancename
func (e *Exporter) Connect(ctx context.Context) error {
	config := e.config

	params, err := godror.ParseConnString(config.Connection)
	if err != nil {
		return err
	}
	params.StandaloneConnection = true

	db := sql.OpenDB(godror.NewConnector(params))
	if err = db.PingContext(ctx); err != nil {
		slog.Info("open db", "params", params, "error", err)
		e.Close()

		return err
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	config.db = db

	query := "SELECT object_name, object_type, object_id FROM all_objects FETCH FIRST 1 ROW ONLY"

	up := e.Query(ctx, query, func(rows *sql.Rows) bool {
		var name, typ string
		var id int64
		err := rows.Scan(&name, &typ, &id)
		if err != nil {
			slog.Error("scan", "query", query, "error", err)
			return false
		}
		slog.Debug("scan", "obj", name, "type", typ, "id", id)

		return false
	})

	if !up {
		return e.Close()
	}
	return nil
}

// Close Connections
func (e *Exporter) Close() error {
	if e.config.db == nil {
		return nil
	}
	slog.Debug("Closing", "connection", e.config.Connection)

	err := e.config.db.Close()
	e.config.db = nil
	if err != nil {
		slog.Error("close", "connection", e.config.Connection, "error", err)
	}
	return err
}

func main() {
	if err := Main(); err != nil {
		slog.Error("main", "error", err)
		os.Exit(1)
	}
}

func Main() error {
	flag.Parse()

	config := &Config{
		Connection: *connection,
	}

	{
		fh, err := os.Create(*memProfFn)
		if err != nil {
			return err
		}
		defer func() {
			if err := pprof.WriteHeapProfile(fh); err != nil {
				slog.Error("WriteHeapProfile", "file", fh.Name(), "error", err)
			}
			if err := fh.Close(); err != nil {
				slog.Error("Close", "file", fh.Name(), "error", err)
			}
		}()
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	ctx, cancel = signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	exporter := NewExporter(config)
	var memstats runtime.MemStats
	for {
		var err error
		if err = exporter.Connect(ctx); err == nil {
			err = exporter.Close()
		}

		runtime.GC()
		runtime.ReadMemStats(&memstats)
		fmt.Println(memstats.Alloc)

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return err
		}

		time.Sleep(500 * time.Millisecond)
	}
	return nil
}
