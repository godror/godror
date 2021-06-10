//go:build gofuzz
// +build gofuzz

// Copyright 2020 The Godror Authors
// Copyright 2016 Tamás Gulácsi
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package num

import "strings"

//go:generate sh -c "cd /tmp && go get -u github.com/dvyukov/go-fuzz/go-fuzz github.com/dvyukov/go-fuzz/go-fuzz-build"
//go:generate go-fuzz-build github.com/godror/godror/num
//go:generate go test -run=Corpus
//go:generate echo -e "\n\tgo-fuzz -bin=./num-fuzz.zip -workdir=/tmp/fuzz\n"

// Fuzz:
// go-fuzz -bin=./num-fuzz.zip -workdir=/tmp/fuzz
func Fuzz(p []byte) int {
	pS := string(p)
	var q [22]byte
	n := OCINum(q[:0])
	if err := n.SetString(pS); err != nil {
		return -1
	}
	s := n.String()
	if s != strings.TrimSpace(pS) {
		return 1
	}
	return 0
}
