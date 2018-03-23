package goracle

//go:generate bash -c "set -x; { curl -L https://github.com/oracle/odpi/archive/v2.2.1.tar.gz || wget -O - https://github.com/oracle/odpi/archive/v2.2.1.zip; } | tar xzvf - odpi-2.2.1/{embed,include,src,CONTRIBUTING.md,LICENSE.md,README.md}"
//go:generate rm -rf odpi
//go:generate mv odpi-2.2.1 odpi

// Version of this driver
const Version = "v2.2.0"
