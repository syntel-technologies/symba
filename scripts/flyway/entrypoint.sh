#!/bin/sh
set -eu

/usr/local/bin/symba-verify-release
exec /flyway/flyway "$@"

