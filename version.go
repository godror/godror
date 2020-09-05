// Copyright 2020 The Godror Authors
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

//go:generate bash -c "echo 4.0.2>odpi-version; set -x; curl -L https://github.com/oracle/odpi/archive/$(echo -n v; cat odpi-version).tar.gz | tar xzvf - odpi-$(cat odpi-version)/{embed,include,src,CONTRIBUTING.md,LICENSE.md,README.md} && cp -a odpi/embed/require.go odpi-$(cat odpi-version)/embed/ && cp -a odpi/include/require.go odpi-$(cat odpi-version)/include/ && cp -a odpi/src/require.go odpi-$(cat odpi-version)/src/ && rm -rf odpi && mv odpi-$(cat odpi-version) odpi; rm -f odpi-{,v}version; git checkout -- $(git status --porcelain -- odpi/*/*.go | sed -n -e '/ D / { s/^ D //;p;}')"

// Version of this driver
const Version = "v0.20.1"
