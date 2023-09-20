//go:build never
// +build never

// Copyright 2021 The Godror Authors

package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"flag"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func main() {
	if err := Main(); err != nil {
		log.Fatalf("%+v", err)
	}
}

func Main() error {
	// flagPackage := flag.String("pkg", "godror", "package name to use")
	flagOut := flag.String("o", "", "output file name")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	b, err := exec.CommandContext(ctx, "go", "env", "GOROOT").Output()
	if err != nil {
		return err
	}
	fn := filepath.Join(string(bytes.TrimSpace(b)), "lib", "time", "zoneinfo.zip")
	log.Printf("go env GOROOT: %s -> fn=%q", b, fn)
	zr, err := zip.OpenReader(fn)
	if err != nil {
		return err
	}
	defer zr.Close()
	names := make([]string, 0, len(zr.File))
	for _, f := range zr.File {
		if f.Mode().IsDir() {
			continue
		}
		names = append(names, strings.ToLower(f.Name)+f.Name)
	}
	sort.Strings(names)
	fh := os.Stdout
	if !(*flagOut == "" || *flagOut == "-") {
		if fh, err = os.Create(*flagOut); err != nil {
			return err
		}
	}
	defer fh.Close()
	bw := bufio.NewWriter(fh)
	for _, name := range names {
		bw.WriteString(name)
		bw.WriteByte('\n')
	}
	return fh.Close()
}
