// Copyright 2020, 2026 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"runtime/debug"
	"strconv"
)

//
// To download the current main, call "VERSION=main go generate"
//
//go:generate bash -c "set -euo pipefail; : ${DOLLAR}{VERSION:=v26.1.0}; DIR=${DOLLAR}{VERSION#v}; echo VERSION=${DOLLAR}VERSION DIR=${DOLLAR}DIR; set -x; curl -L https://github.com/oracle/odpi/archive/refs/${DOLLAR}(if [[ ${DOLLAR}DIR = ${DOLLAR}VERSION ]]; then echo heads; else echo tags; fi)/${DOLLAR}{VERSION}.tar.gz | tar xzvf - odpi-${DOLLAR}DIR/{embed,include,src,CONTRIBUTING.md,README.md,LICENSE.txt} && cp -a odpi/embed/require.go odpi-${DOLLAR}DIR/embed/ && cp -a odpi/include/require.go odpi-${DOLLAR}DIR/include/ && cp -a odpi/src/require.go odpi-${DOLLAR}DIR/src/ && rm -rf odpi && mv odpi-${DOLLAR}DIR odpi; git status --porcelain -- odpi/*/*.go | sed -n -e '/^ D / { s/^ D //;p;}' | xargs -r git checkout -- "

// Version of this driver
func init() {
	if info, ok := debug.ReadBuildInfo(); ok && info != nil {
		for _, m := range info.Deps {
			if m == nil || m.Path != "github.com/godror/godror" {
				continue
			}
			for m.Replace != nil {
				m = m.Replace
			}
			if m.Version != "" {
				Version = m.Version + "+ODPI-" + odpiVersion
			}
			break
		}
	}
}

var (
	godrorVersion = "v0.53.2"

	odpiVersion = strconv.Itoa(DpiMajorVersion) +
		"." + strconv.Itoa(DpiMinorVersion) +
		"." + strconv.Itoa(DpiPatchLevel)

	Version = godrorVersion + "+ODPI-" + odpiVersion
)
