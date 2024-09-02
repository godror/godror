// Copied from github.com/shirou/gopsutil/v4/net/net_linux.go
//
// SPDX-License-Identifier: BSD-3-Clause

package cloexec

import (
	"syscall"
)

type netConnectionKindType struct {
	family   uint32
	sockType uint32
	filename string
}

var kindTCP4 = netConnectionKindType{
	family:   syscall.AF_INET,
	sockType: syscall.SOCK_STREAM,
	filename: "tcp",
}

var kindTCP6 = netConnectionKindType{
	family:   syscall.AF_INET6,
	sockType: syscall.SOCK_STREAM,
	filename: "tcp6",
}

var kindUDP4 = netConnectionKindType{
	family:   syscall.AF_INET,
	sockType: syscall.SOCK_DGRAM,
	filename: "udp",
}

var kindUDP6 = netConnectionKindType{
	family:   syscall.AF_INET6,
	sockType: syscall.SOCK_DGRAM,
	filename: "udp6",
}

var kindUNIX = netConnectionKindType{
	family:   syscall.AF_UNIX,
	filename: "unix",
}

var netConnectionKindMap = map[string][]netConnectionKindType{
	"all":   {kindTCP4, kindTCP6, kindUDP4, kindUDP6, kindUNIX},
	"tcp":   {kindTCP4, kindTCP6},
	"tcp4":  {kindTCP4},
	"tcp6":  {kindTCP6},
	"udp":   {kindUDP4, kindUDP6},
	"udp4":  {kindUDP4},
	"udp6":  {kindUDP6},
	"unix":  {kindUNIX},
	"inet":  {kindTCP4, kindTCP6, kindUDP4, kindUDP6},
	"inet4": {kindTCP4, kindUDP4},
	"inet6": {kindTCP6, kindUDP6},
}
