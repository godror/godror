#!/bin/sh
echo '{ cd $(dirname $0); . env.sh; }' >&2
export TNS_ADMIN="$(dirname "$(find "$(pwd)" -type f -name tnsnames.ora | sort -r | head -n1)")"
export GODROR_TEST_DSN=user=test password=KSyB5a2vTpEKB1WT connectString=db201911301540_high standaloneConnection=1 timezone=UTC
export "LD_LIBRARY_PATH=/usr/lib/oracle/21/client64/lib/:$LD_LIBRARY_PATH"
