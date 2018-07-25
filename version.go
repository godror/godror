package goracle

//go:generate bash -c "set -x; { curl -L https://github.com/oracle/odpi/archive/v2.4.2.tar.gz || wget -O - https://github.com/oracle/odpi/archive/v2.4.2.zip; } | tar xzvf - odpi-2.4.2/{embed,include,src,CONTRIBUTING.md,LICENSE.md,README.md}"
//go:generate rm -rf odpi
//go:generate mv odpi-2.4.2 odpi

// Version of this driver
const Version = "v2.5.8"
