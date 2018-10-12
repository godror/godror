package goracle

//go:generate bash -c "set -x; curl -L https://github.com/oracle/odpi/archive/v3.0.0.tar.gz | tar xzvf - odpi-3.0.0/{embed,include,src,CONTRIBUTING.md,LICENSE.md,README.md}"
//go:generate rm -rf odpi
//go:generate mv odpi-3.0.0 odpi

// Version of this driver
const Version = "v2.9.0"
