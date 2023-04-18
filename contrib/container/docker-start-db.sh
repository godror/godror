#!/usr/bin/env bash
DOCKER="$(command -v podman 2>/dev/null || echo docker)"
DATA="${DATA:-/tmp/oradata}"
mkdir -p "$DATA"
chmod 0777 "$DATA"
set -x
#pwd="$(openssl rand -base64 14)"
#echo "PASSWORD=$pwd"
#echo "$pwd" | podman secret create oracle_pwd -
exec "$DOCKER" run -t -i --rm \
    --name 23c \
    -p "${PORT:-1521}:1521" \
    -v "$DATA:/opt/oracle/oradata" \
    -v "$(cd "$(dirname "$0")" && pwd)/startup:/opt/oracle/scripts/startup" \
    container-registry.oracle.com/database/free
