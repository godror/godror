//go:build never
// +build never

// Copyright 2021 The Godror Authors

package main

import (
	"archive/zip"
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
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
	var buf bytes.Buffer
	buf.WriteByte('\n')
	for _, f := range zr.File {
		if f.Mode().IsDir() {
			continue
		}
		fmt.Fprintf(&buf, "%s\n", f.Name)
	}
	if *flagOut == "" || *flagOut == "-" {
		fmt.Println(buf.String())
		return nil
	}
	return os.WriteFile(*flagOut, buf.Bytes(), 0640)
}
