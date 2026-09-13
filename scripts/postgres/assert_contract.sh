#!/bin/sh
set -eu

fail() {
    echo "symba postgres admission failed: $1" >&2
    exit 78
}

[ "${POSTGRES_USER:-}" = "postgres" ] || fail "bootstrap_user_invalid"
[ "${POSTGRES_DB:-}" = "postgres" ] || fail "bootstrap_database_invalid"
[ "${POSTGRES_INITDB_ARGS:-}" = "--auth-host=scram-sha-256 --auth-local=peer" ] \
    || fail "initdb_auth_contract_invalid"
[ "${POSTGRES_HOST_AUTH_METHOD:-}" = "scram-sha-256" ] \
    || fail "host_auth_contract_invalid"

: "${POSTGRES_PASSWORD:?required}"
: "${SYMBA_POSTGRES_MIGRATION_PASSWORD:?required}"
: "${SYMBA_POSTGRES_RUNTIME_PASSWORD:?required}"

if [ "${#POSTGRES_PASSWORD}" -lt 32 ] \
    || [ "${#SYMBA_POSTGRES_MIGRATION_PASSWORD}" -lt 32 ] \
    || [ "${#SYMBA_POSTGRES_RUNTIME_PASSWORD}" -lt 32 ]; then
    fail "database_secret_too_short"
fi
if [ "$POSTGRES_PASSWORD" = "$SYMBA_POSTGRES_MIGRATION_PASSWORD" ] \
    || [ "$POSTGRES_PASSWORD" = "$SYMBA_POSTGRES_RUNTIME_PASSWORD" ] \
    || [ "$SYMBA_POSTGRES_MIGRATION_PASSWORD" = "$SYMBA_POSTGRES_RUNTIME_PASSWORD" ]; then
    fail "database_secrets_not_independent"
fi

