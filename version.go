// Copyright 2020 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

// https://github.com/oracle/odpi/archive/refs/heads/main.zip
// go:generate bash -c "echo 4.3.0>odpi-version; set -x; curl -L https://github.com/oracle/odpi/archive/refs/tags/v$(cat odpi-version).tar.gz | tar xzvf - odpi-$(cat odpi-version)/{embed,include,src,CONTRIBUTING.md,LICENSE.md,README.md} && cp -a odpi/embed/require.go odpi-$(cat odpi-version)/embed/ && cp -a odpi/include/require.go odpi-$(cat odpi-version)/include/ && cp -a odpi/src/require.go odpi-$(cat odpi-version)/src/ && rm -rf odpi && mv odpi-$(cat odpi-version) odpi; rm -f odpi-{,v}version; git checkout -- $(git status --porcelain -- odpi/*/*.go | sed -n -e '/ D / { s/^ D //;p;}')"
//go:generate bash -c "echo main>odpi-version; set -x; curl -L https://github.com/oracle/odpi/archive/refs/heads/main.tar.gz | tar xzvf - odpi-$(cat odpi-version)/{embed,include,src,CONTRIBUTING.md,LICENSE.md,README.md} && cp -a odpi/embed/require.go odpi-$(cat odpi-version)/embed/ && cp -a odpi/include/require.go odpi-$(cat odpi-version)/include/ && cp -a odpi/src/require.go odpi-$(cat odpi-version)/src/ && rm -rf odpi && mv odpi-$(cat odpi-version) odpi; rm -f odpi-{,v}version; git checkout -- $(git status --porcelain -- odpi/*/*.go | sed -n -e '/ D / { s/^ D //;p;}')"

// Version of this driver
const Version = "v0.30.2"
