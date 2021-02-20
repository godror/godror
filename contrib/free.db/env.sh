#!/bin/sh
echo '{ cd $(dirname $0); . env.sh; }' >&2
export TNS_ADMIN="$(dirname "$(find "$(pwd)" -type f -name tnsnames.ora | sort -r | head -n1)")"
export GODROR_TEST_DSN=user=test password=r97oUPimsmTOIcBaeeDF connectString=db201911301540_high standaloneConnection=1 timezone=UTC
