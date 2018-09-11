package goracle

//go:generate bash -c "set -x; { curl -L https://github.com/oracle/odpi/archive/master.tar.gz || wget -O - https://github.com/oracle/odpi/archive/master.zip; } | tar xzvf - odpi-master/{embed,include,src,CONTRIBUTING.md,LICENSE.md,README.md}"
//go:generate rm -rf odpi
//go:generate mv odpi-master odpi

// Version of this driver
const Version = "v2.7.0"
