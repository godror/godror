#!/bin/sh
echo '{ cd $(dirname $0); . env.sh; }' >&2
export TNS_ADMIN="$(dirname "$(find "$(pwd)" -type f -name tnsnames.ora | sort -r | head -n1)")"
export GODROR_TEST_USERNAME=test
export GODROR_TEST_PASSWORD=r97oUPimsmTOIcBaeeDF
export GODROR_TEST_DB=db201911301540_high
export GODROR_TEST_STANDALONE=1
