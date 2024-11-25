//go:build !posix
// +build !posix

// Copyright 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/godror/godror"
)

// TestConnCut tests prepared statements handling connection cuting.
//
// WARNING: this won't work if the remote needs TLS !!!
func TestConnCut(t *testing.T) {
	if os.Getenv("GODROR_TEST_DB") == "" {
		t.Skip("TestConnCut does not work with the default TLS'd Cloud DB")
	}
	// First, find out the remote address of the connection

	rem1 := make(map[string]net.TCPAddr)
	if err := getRemotes(rem1); err != nil {
		t.Fatal(err)
	}

	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.StandaloneConnection = godror.Bool(true)
	db, err := sql.Open("godror", P.StringWithPassword())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(testContext("ConnCut"), 52*time.Second)
	defer cancel()
	const qry = "SELECT SYS_CONTEXT('userenv', 'service_name') FROM all_objects"
	var upstream net.TCPAddr
	rem2 := make(map[string]net.TCPAddr)
	var serviceName string
	for i := 0; i < 10; i++ {
		for k := range rem2 {
			delete(rem2, k)
		}
		shortCtx, shortCancel := context.WithTimeout(ctx, 5*time.Second)
		rows, err := db.QueryContext(shortCtx, qry)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		rows.Next()
		err = rows.Scan(&serviceName)
		shortCancel()
		if err != nil {
			t.Fatal(err)
		}
		err = getRemotes(rem2)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("service:", serviceName)
		for k := range rem2 {
			if _, ok := rem1[k]; ok {
				delete(rem2, k)
				continue
			}
			upstream = rem2[k]
		}
		if len(rem2) == 1 {
			break
		}
	}
	db.Close()
	if len(rem2) != 1 {
		t.Skipf("cannot find remote address of %q: when connecting to it, %v connections has been created",
			testConStr, rem2)
	}
	t.Log("upstream:", upstream.String())

	t.Parallel()
	// Second, create proxy for it
	px, err := newTCPProxy(ctx, upstream, t)
	if err != nil {
		t.Fatal(err)
	}
	pxCtx, pxCancel := context.WithCancel(ctx)
	defer pxCancel()
	go func() { px.Serve(pxCtx) }()
	P.ConnectString = px.ListenAddr() + "/" + serviceName
	db, err = sql.Open("godror", P.StringWithPassword())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	t.Log("pinging", P.String())
	time.Sleep(100 * time.Millisecond)
	shortCtx, shortCancel := context.WithTimeout(ctx, 3*time.Second)
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			pxCancel()
		case <-done:
			return
		}
	}()
	err = db.PingContext(shortCtx)
	close(done)
	shortCancel()
	if err != nil {
		t.Skip(err)
	}

	// Now the real test
	// 1. prepare statement
	stmt, err := db.PrepareContext(ctx, "SELECT object_name FROM all_objects WHERE ROWNUM <= :2")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// force both connections to be in use
	rows1, err := stmt.QueryContext(ctx, 99)
	if err != nil {
		t.Fatal(err)
	}
	rows2, err := stmt.QueryContext(ctx, 99)
	if err != nil {
		rows1.Close()
		t.Fatal(err)
	}
	rows2.Close()
	rows1.Close()

	for i := 0; i < 10; i++ {
		shortCtx, shortCancel = context.WithTimeout(ctx, 5*time.Second)
		var s string
		err = stmt.QueryRowContext(shortCtx, 1).Scan(&s)
		shortCancel()
		if err != nil {
			if i <= 3 {
				t.Errorf("%d. %v", i+1, err)
			} else {
				t.Logf("%d. %v", i+1, err)
			}
		}
		t.Log(s)

		shortCtx, shortCancel = context.WithTimeout(ctx, 5*time.Second)
		err := db.PingContext(shortCtx)
		shortCancel()
		if err != nil {
			if i <= 3 {
				t.Error(err)
			} else {
				t.Log(err)
			}
		}

		if i == 3 {
			t.Log("canceling proxy")
			go func() {
				pxCancel()
			}()
		}
	}

}

type tcpProxy struct {
	lsnr *net.TCPListener
	*testing.T
	upstream net.TCPAddr
}

func (px tcpProxy) ListenAddr() string { return px.lsnr.Addr().String() }
func newTCPProxy(ctx context.Context, upstream net.TCPAddr, t *testing.T) (*tcpProxy, error) {
	var d net.Dialer
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	conn, err := d.DialContext(ctx, "tcp", upstream.String())
	cancel()
	if err != nil {
		return nil, err
	}
	conn.Close()
	px := tcpProxy{upstream: upstream, T: t}
	px.lsnr, err = net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}) // random port on localhost
	return &px, err
}

func (px *tcpProxy) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		px.lsnr.Close()
	}()
	for {
		down, err := px.lsnr.AcceptTCP()
		if err != nil {
			if px.T != nil {
				px.Log(err)
			}
			var tErr interface{ Temporary() bool }
			if errors.As(err, &tErr) && !tErr.Temporary() {
				return err
			}
			continue
		}
		go px.handleConn(ctx, down)
	}
}
func (px *tcpProxy) handleConn(ctx context.Context, down *net.TCPConn) error {
	defer down.Close()
	up, err := net.DialTCP("tcp", nil, &px.upstream)
	if err != nil {
		if px.T != nil {
			px.Log(err)
		}
		return err
	}
	defer up.Close()
	go func() {
		<-ctx.Done()
		up.Close()
		down.Close()
	}()
	pipe := func(ctx context.Context, dst, src *net.TCPConn) error {
		buf := make([]byte, 512)
		var consecEOF int
		//remote := src.RemoteAddr()
		for {
			if err := ctx.Err(); err != nil {
				dst.Close()
				return err
			}
			n, err := src.Read(buf)
			if n != 0 {
				if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
					return writeErr
				}
			}
			if err == nil {
				consecEOF = 0
			} else if err == io.EOF {
				consecEOF++
				if consecEOF > 3 {
					return err
				}
				time.Sleep(time.Second)
				continue
			} else {
				//consecEOF = 0
				if px.T != nil {
					px.Logf("Copy from %s to %s: %v", src.RemoteAddr(), dst.RemoteAddr(), err)
				}
				return err
			}
		}
	}
	slowCtx, slowCancel := context.WithCancel(context.Background())
	defer slowCancel()
	go func() {
		pipe(ctx, down, up)
		time.Sleep(2 * time.Second)
		slowCancel()
	}()
	return pipe(slowCtx, up, down)
}

// /proc/self/net/tcp 3. col is rem_addr:port
func getRemotes(dest map[string]net.TCPAddr) error {
	for _, nm := range []string{"/proc/self/net/tcp", "/proc/self/net/tcp6"} {
		b, err := os.ReadFile(nm)
		if err != nil {
			return err
		}
		lines := bytes.Split(b, []byte{'\n'})
		if len(lines) < 1 {
			return errors.New("empty " + nm)
		} else if len(lines) < 2 {
			return nil
		}

		for _, line := range lines[1:] {
			fields := bytes.Fields(line)
			if len(fields) <= 2 {
				continue
			}
			var local, remote net.TCPAddr
			if _, err := fmt.Sscanf(string(fields[1])+" "+string(fields[2]), "%X:%X %X:%X",
				&local.IP, &local.Port, &remote.IP, &remote.Port,
			); err != nil {
				return err
			}
			if remote.Port != 0 {
				reverseBytes(local.IP)
				reverseBytes(remote.IP)
				dest[local.String()] = remote
			}
		}
	}
	return nil
}

func reverseBytes(p []byte) {
	for i, j := 0, len(p)-1; i < j; i, j = i+1, j-1 {
		p[i], p[j] = p[j], p[i]
	}
}
