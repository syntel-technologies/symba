#!/bin/sh
set -eu

/usr/local/bin/symba-verify-release
/usr/local/bin/symba-assert-postgres-contract

exec /usr/local/bin/docker-entrypoint.sh "$@"
