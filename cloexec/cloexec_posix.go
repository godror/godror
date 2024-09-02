//go:build !windows

// Copyright 2024 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package cloexec

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func setFd(fd uintptr, set bool) error {
	mask := syscall.FD_CLOEXEC
	if !set {
		mask = ^mask
	}
	_, err := unix.FcntlInt(uintptr(fd), syscall.F_SETFD, mask)
	return err
}
func getFd(fd uintptr) (bool, error) {
	rc, err := unix.FcntlInt(uintptr(fd), syscall.F_GETFD, 0) // arg is ignored
	if testLogf != nil {
		testLogf("getFd(%d): (%d,%+v)", fd, rc, err)
	}
	return rc&syscall.FD_CLOEXEC != 0, err
}

func getConnections(kind string) ([]uint32, error) {
	if kind == "" {
		kind = "tcp"
	}
	dn := fmt.Sprintf("/proc/%d", os.Getpid())
	dis, err := os.ReadDir(dn + "/fd")
	if err != nil {
		return nil, err
	}
	var kinds []netConnectionKindType
	// var inodes map[string][]uint32
	var inodes map[string]struct{}
	if kind != "all" {
		kinds = netConnectionKindMap[kind]
		// inodes = make(map[string][]uint32)
		inodes = make(map[string]struct{})
		for _, kind := range kinds {
			b, err := os.ReadFile(dn + "/net/" + kind.filename)
			if err != nil {
				return nil, err
			}
			var idx int
			// pid=$(pidof firefox-bin); for nm in tcp tcp6 udp udp6 unix; do echo -n "$nm: "; head -n1 /proc/$pid/net/$nm | tr '[:upper:]' '[:lower:]' | sed -ne '/inode/ { s/inode.*$/inode/; s/[ \t][ \t][ \t]*/ /g; s/tx_queue rx_queue/tx_rx_queue/; s/tr tm->when/trtm_when/; s/^ *//; p; }' | tr -dc ' '| wc -c; done
			switch kind.filename {
			case "tcp", "tcp6", "udp", "udp6":
				idx = 9
			case "unix":
				idx = 6
			default:
				return nil, fmt.Errorf("unknown kind %q", kind)
			}
			for _, line := range bytes.Split(b, []byte("\n"))[1:] {
				if len(line) == 0 {
					continue
				}
				inodes[string(bytes.Fields(line)[idx])] = struct{}{}
			}
		}
	}
	if testLogf != nil {
		testLogf("inodes for %q: %q", kind, inodes)
	}

	var fds []uint32
	for _, di := range dis {
		if di.Type()&fs.ModeSymlink == 0 {
			continue
		}
		fd, err := strconv.ParseUint(di.Name(), 10, 32)
		if err != nil {
			continue
		}
		if lnk, err := os.Readlink(dn + "/fd/" + di.Name()); err != nil {
			if testLogf != nil && !os.IsNotExist(err) {
				testLogf("%+v", err)
			}
			continue
		} else if rest, ok := strings.CutPrefix(lnk, "socket:["); ok {
			rest = rest[:len(rest)-1] // ]
			if ok = len(kinds) == 0; !ok {
				_, ok = inodes[rest]
			}
			if ok {
				// inodes[rest] = append(inodes[rest], uint32(fd))
				fds = append(fds, uint32(fd))
			} else if testLogf != nil {
				testLogf("inode %q not found", rest)
			}
		}
	}
	// if testLogf != nil {
	// 	testLogf("inodes for %q: %v", kind, inodes)
	// }
	return fds, nil
}
