export TNS_ADMIN="$(dirname "$(find "$PWD" -type f -name tnsnames.ora | sort -r | head -n1)")"
export GORACLE_DRV_TEST_USERNAME=test
export GORACLE_DRV_TEST_PASSWORD=r97oUPimsmTOIcBaeeDF
export GORACLE_DRV_TEST_DB=goracle_high
export GORACLE_DRV_TEST_STANDALONE=1
