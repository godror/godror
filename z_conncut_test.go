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
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"syscall"
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

	db, err := sql.Open("godror", testConStr)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shortCtx, shortCancel := context.WithTimeout(ctx, 3*time.Second)
	const qry = "SELECT SYS_CONTEXT('userenv', 'service_name') FROM DUAL"
	var serviceName string
	err = db.QueryRowContext(shortCtx, qry).Scan(&serviceName)
	shortCancel()
	if err != nil {
		t.Fatal(err)
	}
	rem2 := make(map[string]net.TCPAddr)
	err = getRemotes(rem2)
	db.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("service:", serviceName)
	var upstream net.TCPAddr
	for k := range rem2 {
		if _, ok := rem1[k]; ok {
			delete(rem2, k)
			continue
		}
		upstream = rem2[k]
	}
	if len(rem2) != 1 {
		t.Fatalf("cannot find remote address of %q: when connecting to it, %v connections has been created",
			testConStr, rem2)
	}
	t.Log("upstream:", upstream.String())

	// Second, create proxy for it
	var px tcpProxy
	if false {
		px, err = newTCPProxy(ctx, upstream)
	} else if false {
		px, err = newNCProxy(upstream.String())
	} else {
		px, err = newSocatProxy(upstream.String())
	}
	if err != nil {
		t.Fatal(err)
	}
	pxCtx, pxCancel := context.WithCancel(ctx)
	defer pxCancel()
	go func() { px.Serve(pxCtx) }()
	P, err := godror.ParseConnString(testConStr)
	if err != nil {
		t.Fatal(err)
	}
	P.DSN = px.ListenAddr() + "/" + serviceName
	db, err = sql.Open("godror", P.StringWithPassword())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	t.Log("pinging", P.String())
	time.Sleep(100 * time.Millisecond)
	shortCtx, shortCancel = context.WithTimeout(ctx, 3*time.Second)
	err = db.PingContext(shortCtx)
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

	for i := 0; i < 10; i++ {
		shortCtx, shortCancel = context.WithTimeout(ctx, 3*time.Second)
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

		if i == 3 {
			t.Log("canceling proxy")
			go func() {
				time.Sleep(50 * time.Millisecond)
				pxCancel()
			}()
		}
	}

}

type tcpProxy interface {
	ListenAddr() string
	Serve(context.Context) error
}

type ncTCPProxy struct {
	lsnr           *net.TCPListener
	addr           string
	upstream       string
	cmdDown, cmdUp *exec.Cmd
}

func newNCProxy(upstream string) (*ncTCPProxy, error) {
	lsnr, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		return nil, err
	}
	defer lsnr.Close()

	addr := lsnr.Addr().String()
	_, downPort, _ := net.SplitHostPort(addr)
	upHost, upPort, err := net.SplitHostPort(upstream)
	if err != nil {
		return nil, err
	}
	pw, pr, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	cmdDown := exec.Command("nc", "-l", "-k", "-v", "-v", "-w", "-1", "-l", "-p", downPort, "127.0.0.1")
	cmdDown.Stderr = os.Stderr
	cmdDown.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: 9, Setpgid: true}
	cmdDown.Stdin, cmdDown.Stdout = pw, pr

	cmdUp := exec.Command("nc", "-v", "-v", "-w", "-1", "-k", upHost, upPort)
	cmdUp.Stdout, cmdUp.Stderr = os.Stdout, os.Stderr
	cmdUp.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: 9, Setpgid: true}
	cmdUp.Stdin, cmdUp.Stdout = pr, pw
	if err = cmdUp.Start(); err != nil {
		pw.Close()
		return nil, err
	}

	return &ncTCPProxy{upstream: upstream, addr: addr, cmdUp: cmdUp, cmdDown: cmdDown, lsnr: lsnr}, nil
}
func (px ncTCPProxy) ListenAddr() string { return px.addr }
func (px *ncTCPProxy) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		p, _ := os.FindProcess(-px.cmdUp.Process.Pid)
		p.Kill()
		px.cmdUp.Process.Kill()
	}()
	px.lsnr.Close()
	return px.cmdDown.Run()
}

type socatTCPProxy struct {
	addr     string
	port     int
	upstream string
	cmd      *exec.Cmd
}

func newSocatProxy(upstream string) (*socatTCPProxy, error) {
	lsnr, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		return nil, err
	}
	addr := lsnr.Addr().String()
	_, p, _ := net.SplitHostPort(addr)
	port, err := strconv.ParseInt(p, 10, 32)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(
		"socat", "-d", "-d", "-T10",
		"TCP-LISTEN:"+strconv.Itoa(int(port))+",bind=localhost,fork,range=127.0.0.1/32,shut-none,linger2=3",
		"TCP:"+upstream,
	)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: 9, Setpgid: true}
	lsnr.Close()
	return &socatTCPProxy{upstream: upstream, addr: addr, port: int(port), cmd: cmd}, cmd.Start()
}
func (px socatTCPProxy) ListenAddr() string { return px.addr }
func (px *socatTCPProxy) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		p, _ := os.FindProcess(-px.cmd.Process.Pid)
		p.Kill()
		px.cmd.Process.Kill()
	}()
	return px.cmd.Run()
}

type goTCPProxy struct {
	upstream net.TCPAddr
	lsnr     *net.TCPListener
}

func (px goTCPProxy) ListenAddr() string { return px.lsnr.Addr().String() }
func newTCPProxy(ctx context.Context, upstream net.TCPAddr) (*goTCPProxy, error) {
	var d net.Dialer
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	conn, err := d.DialContext(ctx, "tcp", upstream.String())
	cancel()
	if err != nil {
		return nil, err
	}
	conn.Close()
	px := goTCPProxy{upstream: upstream}
	px.lsnr, err = net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}) // random port on localhost
	return &px, err
}

func (px *goTCPProxy) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		px.lsnr.Close()
	}()
	for {
		down, err := px.lsnr.AcceptTCP()
		if err != nil {
			log.Println(err)
			var tErr interface{ Temporary() bool }
			if errors.As(err, &tErr) && !tErr.Temporary() {
				return err
			}
			continue
		}
		go px.handleConn(ctx, down)
	}
}
func (px *goTCPProxy) handleConn(ctx context.Context, down *net.TCPConn) error {
	defer down.Close()
	up, err := net.DialTCP("tcp", nil, &px.upstream)
	if err != nil {
		log.Println(err)
		return err
	}
	defer up.Close()
	pipe := func(dst, src *net.TCPConn) error {
		defer dst.Close()
		_, err := io.Copy(dst, src)
		log.Printf("Copy from %s to %s: %v", src.RemoteAddr(), dst.RemoteAddr(), err)
		return err
	}
	go pipe(up, down)
	return pipe(down, up)
}

// /proc/self/net/tcp 3. col is rem_addr:port
func getRemotes(dest map[string]net.TCPAddr) error {
	for _, nm := range []string{"/proc/self/net/tcp", "/proc/self/net/tcp6"} {
		b, err := ioutil.ReadFile(nm)
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
