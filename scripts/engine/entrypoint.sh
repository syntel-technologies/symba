#!/bin/sh
set -eu

/usr/local/bin/symba-verify-release
exec python -m symba.main "$@"

